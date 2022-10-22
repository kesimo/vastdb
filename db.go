// Package vastDB implements a generic in-memory Key/value store
// It persists to disk, is ACID compliant, has internal RW locking mechanisms,
// and is optimized for speed and memory usage and supports multi-field indexing for Generic types

package vastdb

import (
	"errors"
	"fmt"
	"github.com/kesimo/vastdb/internal/tree"
	"io"
	"reflect"
	"sync"
	"time"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an only on-memory database
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")

	// ErrEmptyKey is returned when an empty key is provided.
	ErrEmptyKey = errors.New("empty key")
)

// DB represents a collection of Key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB[T any] struct {
	mu          sync.RWMutex           // the gatekeeper for all fields
	persistence *persistence[T]        // persistence layer
	keys        tree.Btree[*dbItem[T]] // a tree of all item ordered by Key
	exps        tree.Btree[*dbItem[T]] // a tree of items ordered by expiration
	indices     map[string]*index[T]   // map containing all indices
	insIndices  []*index[T]            // a reuse buffer for gathering indices
	closed      bool                   // set when the database has been closed
	config      Config[T]              // the database configuration
	isBiType    bool                   // is the type T a builtin type
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast, and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond SyncPolicy = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always SyncPolicy = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config[T any] struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a Key
	// has been expired.
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is
	// performing the deletion of expired items. If OnExpired is present then
	// this callback will not be called. If this callback is present, then the
	// deletion of the timed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key string, value T, tx *Tx[T]) error
}

// expirationCtx is a simple b-tree context for ordering by expiration.
type expirationCtx[T any] struct {
	db *DB[T]
}

func checkNoVisibleFields[T any](a T) error {
	visibleFields := 0
	rt := reflect.TypeOf(a) // take type of input
	if rt == nil {
		return fmt.Errorf("vastdb: type is nil")
	}
	if rt.Kind() == reflect.Interface {
		return fmt.Errorf("vastdb: interface type %s is not supported", rt)
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem() // use Elem to get the pointed-to-type
	}
	if rt.Kind() == reflect.Slice {
		rt = rt.Elem() // use Elem to get type of slice's element
	}
	if rt.Kind() == reflect.Ptr { // handle input of type like []*StructType
		rt = rt.Elem() // use Elem to get the pointed-to-type
	}
	if rt.Kind() == reflect.Map {
		return nil
	}
	if rt.Kind() != reflect.Struct {
		if rt.Name() == "" {
			return fmt.Errorf("vastdb: %v type is not supported", rt)
		}
		return nil
	}
	for _, f := range reflect.VisibleFields(rt) {
		if f.IsExported() {
			visibleFields++
		}
	}
	if visibleFields == 0 {
		return fmt.Errorf("vastdb: struct must not have any exported fields")
	}
	return nil
}

// checkTypeStruct checks if the type is a struct or a pointer to a struct.
// supports only Single-Nested Structs, e.g. pointer to a struct or slice of structs or slice of pointers to structs
func checkTypeStruct[T any](a T) bool {
	rt := reflect.TypeOf(a) // take type of input
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem() // use Elem to get the pointed-to-type
	}
	if rt.Kind() == reflect.Slice {
		rt = rt.Elem() // use Elem to get type of slice's element
	}
	if rt.Kind() == reflect.Ptr { // handle input of type like []*StructType
		rt = rt.Elem() // use Elem to get the pointed-to-type
	}
	if rt.Kind() != reflect.Struct {
		return false
	}
	return true
}

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open[T any](path string, typeObject ...T) (*DB[T], error) {
	var checkObj T
	if len(typeObject) > 1 {
		checkObj = typeObject[0]
	} else {
		checkObj = *new(T)
	}
	err := checkNoVisibleFields(checkObj)
	if err != nil {
		return nil, err
	}
	db := &DB[T]{}
	// initialize trees and indexes
	db.keys = tree.NewGBtree[*dbItem[T]](lessCtx[T](nil))
	db.exps = tree.NewGBtree[*dbItem[T]](lessCtx[T](&expirationCtx[T]{db}))
	db.indices = make(map[string]*index[T])
	// initialize default configuration
	db.config = Config[T]{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// configure the persistence layer
	db.persistence = &persistence[T]{
		DB: db,
		// disable persistence if no path or :memory: provided
		isActive:             path != ":memory:" && path != "",
		path:                 path,
		AutoShrinkPercentage: db.config.AutoShrinkPercentage,
		AutoShrinkMinSize:    db.config.AutoShrinkMinSize,
	}
	err = db.persistence.Open()
	if err != nil {
		return nil, err
	}
	// pre-check if type T is Built-In Type to speed up RW to disk
	db.isBiType = checkTypeStruct(checkObj)
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB[T]) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	err := db.persistence.Close()
	if err != nil && err != ErrSyncFile {
		// ignore sync error and close file
		return err
	}
	// set all references to nil in order to prevent later usage
	db.keys, db.exps, db.indices, db.persistence = nil, nil, nil, nil
	return nil
}

// Snapshot writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases. For persistent databases, the database file can be copied
func (db *DB[T]) Snapshot(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	// use a buffered writer and txCommit every 4MB
	var buf []byte
	now := time.Now()
	// write every item of the "keys"-Tree to file
	db.keys.Ascend(func(item *dbItem[T]) bool {
		dbi := item
		buf = dbi.writeSetTo(buf, now, db.isBiType)
		if len(buf) > 1024*1024*4 {
			// txCommit when buffer is over 4MB
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final txCommit
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases
func (db *DB[T]) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persistence.isActive {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	_, err := db.persistence.LoadFromReader(rd, time.Now())
	return err
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in a b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *DB[T]) CreateIndex(name, pattern string,
	less ...func(a, b T) bool) error {
	return db.Update(func(tx *Tx[T]) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// The items are ordered in a b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// If a previous index with the same name exists, that index will be deleted.
func (db *DB[T]) ReplaceIndex(name, pattern string,
	less ...func(a, b T) bool) error {
	return db.Update(func(tx *Tx[T]) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index.
func (db *DB[T]) DropIndex(name string) error {
	return db.Update(func(tx *Tx[T]) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
func (db *DB[T]) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx[T]) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
func (db *DB[T]) ReadConfig(config *Config[T]) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB[T]) SetConfig(config Config[T]) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase inserts an item into the main tree (optional in expire tree) and update every index.
// if an item with same key already exists it will be replaced (using the dbItem.key value to compare)
// and the old item will be returned.
func (db *DB[T]) insertIntoDatabase(item *dbItem[T]) *dbItem[T] {
	if item == nil {
		panic("item cannot be nil")
	}
	var pdbi *dbItem[T]
	// Generate a list of indexes that this item will be inserted in to.
	idxs := db.insIndices
	for _, idx := range db.indices {
		if idx.match(item.key) {
			idxs = append(idxs, idx)
		}
	}
	prev, _ := db.keys.Set(&item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = *prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			db.exps.Delete(&pdbi)
		}
		for _, idx := range idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(&pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.Set(&item)
	}
	for i, idx := range idxs {
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.Set(&item)
		}
		// clear the index
		idxs[i] = nil
	}
	// reuse the index list slice
	db.insIndices = idxs[:0]
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes an item from the keys and exps trees and every index. The input
// item must only have the Key field specified "&dbItem{Key: Key}".
// if item found it deletes the item and returns it
// if item not found it returns nil
func (db *DB[T]) deleteFromDatabase(item *dbItem[T]) *dbItem[T] {
	var pdbi *dbItem[T]
	prev, _ := db.keys.Delete(&item)
	if prev != nil {
		pdbi = *prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			db.exps.Delete(&pdbi)
		}
		for _, idx := range db.indices {
			if !idx.match(pdbi.key) {
				continue
			}
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(&pdbi)
			}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB[T]) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		var onExpired func([]string)
		var expired []*dbItem[T]
		var onExpiredSync func(key string, value T, tx *Tx[T]) error
		err := db.Update(func(tx *Tx[T]) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			var err error
			shrink, err = db.persistence.AutoShrink()
			if err != nil {
				return err
			}
			// get expired items from the expires tree
			expItem := &dbItem[T]{opts: &dbItemOpts{ex: true, exat: time.Now()}}
			db.exps.AscendLT(&expItem, func(item *dbItem[T]) bool {
				expired = append(expired, item)
				return true
			})
			if onExpired == nil && onExpiredSync == nil {
				for _, itm := range expired {
					if _, err := tx.Delete(itm.key); err != nil {
						// it's ok to get a "not found" because the
						// 'Delete' method reports "not found" for
						// expired items.
						if err != ErrNotFound {
							return err
						}
					}
				}
			} else if onExpiredSync != nil {
				for _, itm := range expired {
					if err := onExpiredSync(itm.key, itm.val, tx); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		if onExpired != nil && len(expired) > 0 {
			keys := make([]string, 0, 32)
			for _, itm := range expired {
				keys = append(keys, itm.key)
			}
			onExpired(keys)
		}
		// execute a disk sync, if needed
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			if db.persistence.isActive && db.config.SyncPolicy == EverySecond &&
				flushes != db.persistence.flushes {
				_ = db.persistence.FileSync()
				flushes = db.persistence.flushes
			}
		}()
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
func (db *DB[T]) Shrink() error {
	return db.persistence.Shrink()
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB[T]) managed(writable bool, fn func(tx *Tx[T]) error) (err error) {
	var tx *Tx[T]
	tx, err = db.begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must roll back.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB[T]) View(fn func(tx *Tx[T]) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB[T]) Update(fn func(tx *Tx[T]) error) error {
	return db.managed(true, fn)
}

// Get returns the value for a given key. If the key does not exist then nil,
// It's a wrapper around View with a single Get Read-transaction call.
// ErrNotFound is returned if the key does not exist.
func (db *DB[T]) Get(key string) (*T, error) {
	var val *T
	err := db.View(func(tx *Tx[T]) error {
		var err error
		val, err = tx.Get(key, false)
		return err
	})
	return val, err
}

// Set sets the value for a given key. It's a wrapper around Update with a
// single Set Write-transaction call.
// Return previous value if any.
func (db *DB[T]) Set(key string, val T, options ...*SetOptions) (prev *T, err error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	var p *T
	if len(options) > 0 {
		err = db.Update(func(tx *Tx[T]) error {
			var err error
			p, _, err = tx.Set(key, val, options[0])
			return err
		})
	} else {
		err = db.Update(func(tx *Tx[T]) error {
			var err error
			p, _, err = tx.Set(key, val, nil)
			return err
		})
	}
	return p, err
}

// Del removes the value for a given key. It's a wrapper around Update with a
// single Delete Write-transaction call.
// Return previous value if any.
func (db *DB[T]) Del(key string) (prev *T, err error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	var p *T
	err = db.Update(func(tx *Tx[T]) error {
		var err error
		p, err = tx.Delete(key)
		return err
	})
	return p, err
}

// get return an item or nil if not found.
func (db *DB[T]) get(key string) *dbItem[T] {
	keyItem := &dbItem[T]{key: key}
	item, _ := db.keys.Get(&keyItem)
	if item != nil {
		return *item
	}
	return nil
}

// begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB[T]) begin(writable bool) (*Tx[T], error) {
	tx := &Tx[T]{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		tx.wc = &txWriteContext[T]{}
		tx.wc.rollbackItems = make(map[string]*dbItem[T])
		tx.wc.rollbackIndexes = make(map[string]*index[T])
		if db.persistence.isActive {
			tx.wc.commitItems = make(map[string]*dbItem[T])
		}
	}
	return tx, nil
}

// Len returns the number of items in the database
func (db *DB[T]) Len() (int, error) {
	if db.closed {
		return 0, ErrDatabaseClosed
	}
	if db.keys == nil {
		return 0, ErrNotFound
	}
	return db.keys.Len(), nil
}

func lessCtx[T any](ctx interface{}) func(a, b *dbItem[T]) bool {
	return func(a, b *dbItem[T]) bool {
		return a.Less(b, ctx)
	}
}
