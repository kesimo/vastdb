// Package vastdb implements a low-level in-memory Key/value store in pure Go.
// It persists to disk, is ACID compliant, and uses locking for multiple
// readers and a single writer. Bunt is ideal for projects that need a
// dependable database, and favor speed over data size.
package vastdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
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

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

func panicErr(err error) error {
	panic(fmt.Errorf("vastdb: %w", err))
}

// DB represents a collection of Key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB[T any] struct {
	mu        sync.RWMutex              // the gatekeeper for all fields
	file      *os.File                  // the underlying file
	buf       []byte                    // a buffer to write to
	keys      *btree.BTreeG[*dbItem[T]] // a tree of all item ordered by Key
	exps      *btree.BTreeG[*dbItem[T]] // a tree of items ordered by expiration
	idxs      map[string]*index[T]      // the index trees.
	insIdxs   []*index[T]               // a reuse buffer for gathering indexes
	flushes   int                       // a count of the number of disk flushes
	closed    bool                      // set when the database has been closed
	config    Config[T]                 // the database configuration
	persist   bool                      // do we write to disk
	shrinking bool                      // when an aof shrink is in-process.
	lastaofsz int                       // the size of the last shrink aof size
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
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
	// deletion of the timeed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key string, value T, tx *Tx[T]) error
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx[T any] struct {
	db *DB[T]
}

func checkNoVisibleFields[T any](a T) error {
	visibleFields := 0
	rt := reflect.TypeOf(a) // take type of a
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
		return fmt.Errorf("vastdb: invalid type %T", a)
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

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open[T any](path string, a T) (*DB[T], error) {
	err := checkNoVisibleFields(a)
	if err != nil {
		return nil, err
	}
	db := &DB[T]{}
	// initialize trees and indexes
	db.keys = gBtreeNew[*dbItem[T]](lessCtx[T](nil))
	db.exps = gBtreeNew[*dbItem[T]](lessCtx[T](&exctx[T]{db}))
	db.idxs = make(map[string]*index[T])
	// initialize default configuration
	db.config = Config[T]{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hard coding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
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
	if db.persist {
		_ = db.file.Sync() // do a sync but ignore the error
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

// Save writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases using the ":memory:". Database that persist to disk
// can be snapshotted by simply copying the database file.
func (db *DB[T]) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	// use a buffered writer and flush every 4MB
	var buf []byte
	now := time.Now()
	// iterated through every item in the database and write to the buffer
	gBtreeAscend[*dbItem[T]](db.keys, func(item *dbItem[T]) bool {
		dbi := item
		buf = dbi.writeSetTo(buf, now)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
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
	// one final flush
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
func (db *DB[T]) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persist {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	_, err := db.readLoad(rd, time.Now())
	return err
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
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

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same Key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB[T]) insertIntoDatabase(item *dbItem[T]) *dbItem[T] {
	var pdbi *dbItem[T]
	// Generate a list of indexes that this item will be inserted in to.
	idxs := db.insIdxs
	for _, idx := range db.idxs {
		if idx.match(item.key) {
			idxs = append(idxs, idx)
		}
	}
	prev := gBtreeSetHint[*dbItem[T]](db.keys, &item, nil)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = *prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			gBtreeDeleteHint(db.exps, &pdbi)
		}
		for _, idx := range idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				gBtreeDeleteHint(idx.btr, &pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		gBtreeSetHint(db.exps, &item, nil)
	}
	for i, idx := range idxs {
		if idx.btr != nil {
			// Add new item to btree index.
			gBtreeSetHint(idx.btr, &item, nil)
		}
		// clear the index
		idxs[i] = nil
	}
	// reuse the index list slice
	db.insIdxs = idxs[:0]
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the Key field specified thus "&dbItem{Key: Key}" is all
// that is needed to fully remove the item with the matching Key. If an item
// with the matching Key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB[T]) deleteFromDatabase(item *dbItem[T]) *dbItem[T] {
	var pdbi *dbItem[T]
	prev := gBtreeDeleteHint(db.keys, &item)
	if prev != nil {
		pdbi = *prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if !idx.match(pdbi.key) {
				continue
			}
			if idx.btr != nil {
				// Remove it from the btree index.
				gBtreeDeleteHint(idx.btr, &pdbi)
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
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			// produce a list of expired items that need removing
			expItem := &dbItem[T]{opts: &dbItemOpts{ex: true, exat: time.Now()}}
			gBtreeAscendLessThan(db.exps, &expItem, func(item *dbItem[T]) bool {
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
			if db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				_ = db.file.Sync()
				flushes = db.flushes
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
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	filename := db.file.Name()
	tmp := filename + ".tmp"
	// the endPosition is used to return to the end of the file when we are
	// finished writing all the current items.
	endPosition, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmp)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			now := time.Now()
			pivItem := &dbItem[T]{key: pivot}
			gBtreeAscendGreaterOrEqual(db.keys, &pivItem,
				func(item *dbItem[T]) bool {
					dbi := item
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf, now)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endPosition, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here are awful. So just panic.
		if err := os.Rename(tmp, filename); err != nil {
			_ = panicErr(err)
		}
		db.file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			_ = panicErr(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader, should be no greater than
// the current time.Now().
// Returns the number of bytes of the last command read and the error if any.
func (db *DB[T]) readLoad(rd io.Reader, modTime time.Time) (n int64, err error) {
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()
	totalSize := int64(0)
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// peek at the first byte. If it's a 'nul' control character then
		// ignore it and move to the next byte.
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return totalSize, err
		}
		if c == 0 {
			// ignore nul control characters
			n += 1
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return totalSize, err
		}

		// read a single command.
		// first we should read the number of parts that the of the command
		cmdByteSize := int64(0)
		line, err := r.ReadBytes('\n')
		if err != nil {
			return totalSize, err
		}
		if line[0] != '*' {
			return totalSize, ErrInvalid
		}
		cmdByteSize += int64(len(line))

		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return totalSize, ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return totalSize, ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return totalSize, ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return totalSize, err
			}
			if line[0] != '$' {
				return totalSize, ErrInvalid
			}
			cmdByteSize += int64(len(line))
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return totalSize, ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return totalSize, ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return totalSize, ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return totalSize, err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return totalSize, ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
			cmdByteSize += int64(n + 2)
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return totalSize, ErrInvalid
			}
			if len(parts) == 5 {
				if strings.ToLower(parts[3]) != "ex" {
					return totalSize, ErrInvalid
				}
				ex, err := strconv.ParseUint(parts[4], 10, 64)
				if err != nil {
					return totalSize, err
				}
				now := time.Now()
				dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
				if dur > 0 {
					var valT T
					if err := valueFromString(parts[2], &valT); err != nil {
						return totalSize, err
					}
					db.insertIntoDatabase(&dbItem[T]{
						key: parts[1],
						val: valT,
						opts: &dbItemOpts{
							ex:   true,
							exat: now.Add(dur),
						},
					})
				}
			} else {
				var valT T
				if err := valueFromString(parts[2], &valT); err != nil {
					return totalSize, err
				}
				db.insertIntoDatabase(&dbItem[T]{key: parts[1], val: valT})
			}
		} else if (parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			db.deleteFromDatabase(&dbItem[T]{key: parts[1]})
		} else if (parts[0][0] == 'f' || parts[0][0] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			db.keys = gBtreeNew[*dbItem[T]](lessCtx[T](nil))
			db.exps = gBtreeNew[*dbItem[T]](lessCtx[T](&exctx[T]{db}))
			db.idxs = make(map[string]*index[T])
		} else {
			return totalSize, ErrInvalid
		}
		totalSize += cmdByteSize
	}
}

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB[T]) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	n, err := db.readLoad(db.file, fi.ModTime())
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// The db file has ended mid-command, which is allowed but the
			// data file should be truncated to the end of the last valid
			// command
			if err := db.file.Truncate(n); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := db.file.Seek(n, 0); err != nil {
		return err
	}
	var estaofsz int
	gBtreeWalk(db.keys, func(items []*dbItem[T]) {
		for _, v := range items {
			estaofsz += v.estAOFSetSize()
		}
	})
	db.lastaofsz += estaofsz
	return nil
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

// get return an item or nil if not found.
func (db *DB[T]) get(key string) *dbItem[T] {
	keyItem := &dbItem[T]{key: key}
	item := gBtreeGetHint[*dbItem[T]](db.keys, &keyItem, nil)
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
		if db.persist {
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
