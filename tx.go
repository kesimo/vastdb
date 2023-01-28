// This file is part of the vastDB project.
// Last modified : Kevin Eder
// Creation date: 10.10.2022
// License: MIT
// Use of this source code is governed by a MIT license that can be found in the LICENSE file
// at https://github.com/kesimo/vastdb/blob/main/LICENSE

package vastdb

import (
	"github.com/kesimo/vastdb/internal/tree"
	"github.com/tidwall/match"
	"sort"
	"strings"
	"time"
)

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx[T any] struct {
	db       *DB[T]             // the underlying database.
	writable bool               // when false mutable operations fail.
	funcd    bool               // when true Commit and Rollback panic.
	wc       *txWriteContext[T] // context for writable transactions.
}

type txWriteContext[T any] struct {
	// rollback when deleteAll is called
	rbKeys          tree.Store[*dbItem[T]] // a tree of all item ordered by Key
	rbExps          tree.Store[*dbItem[T]] // a tree of items ordered by expiration
	rbIdxs          map[string]*index[T]   // the index trees.
	rollbackItems   map[string]*dbItem[T]  // details for rolling back tx.
	commitItems     map[string]*dbItem[T]  // details for committing tx.
	iterCount       int                    // stack of iterators
	rollbackIndexes map[string]*index[T]   // details for dropped indexes.
}

// DeleteAll deletes all items from the database.
func (tx *Tx[T]) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	if tx.wc.rbKeys == nil {
		// backup the live data for rollback
		tx.wc.rbKeys = tx.db.keys
		tx.wc.rbExps = tx.db.exps
		tx.wc.rbIdxs = tx.db.indices
	}

	// now reset the live database trees
	tx.db.keys = tree.NewGBtree[*dbItem[T]](lessCtx[T](nil))
	tx.db.exps = tree.NewGBtree[*dbItem[T]](lessCtx[T](&expirationCtx[T]{tx.db}))
	tx.db.indices = make(map[string]*index[T])
	// finally re-create the indexes
	for name, idx := range tx.wc.rbIdxs {
		tx.db.indices[name] = idx.clearCopy()
	}
	// always clear out the commits
	tx.wc.commitItems = make(map[string]*dbItem[T])
	return nil
}

// lock locks the database based on the transaction type.
func (tx *Tx[T]) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock the database based on the transaction type.
func (tx *Tx[T]) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx[T]) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbKeys != nil {
		tx.db.keys = tx.wc.rbKeys
		tx.db.indices = tx.wc.rbIdxs
		tx.db.exps = tx.wc.rbExps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&dbItem[T]{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.indices, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			tx.db.indices[name] = idx
			idx.rebuild()
		}
	}
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx[T]) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	err = tx.db.persistence.txCommit(tx, err)
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx[T]) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() Key-value will expire
	Expires bool
	// TTL is how much time the Key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// GetLess returns the comparator function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no comparator
// function bound to the index
func (tx *Tx[T]) GetLess(index string) (func(a, b T) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.indices[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// Set inserts or replaces an item in the database based on the Key.
// The opts param may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operation replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx[T]) Set(key string, value T, opts *SetOptions) (previous *T,
	replaced bool, err error) {
	if tx.db == nil {
		return nil, false, ErrTxClosed
	} else if !tx.writable {
		return nil, false, ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return nil, false, ErrTxIterating
	}
	item := &dbItem[T]{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)

	var previousValue *T

	// insert into the rollback map if there has not been a deleteAll.
	if tx.wc.rbKeys == nil {
		if prev == nil {
			// An item with the same Key did not previously exist. Let's
			// create a rollback entry with a nil value. A nil value indicates
			// that the entry should be deleted on rollback. When the value is
			// *not* nil, that means the entry should be reverted.
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = nil
			}
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same Key.
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = prev
			}
			if !prev.expired() {
				previousValue, replaced = &(prev.val), true
			}
		}
	}
	// For commits, we simply assign the item to the map. We use this map to
	// write the entry to disk.
	if tx.db.persistence.isActive {
		tx.wc.commitItems[key] = item
	}
	return previousValue, replaced, nil
}

// Get returns a value for a Key. If the item does not exist or if the item
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// the found value will be returned even if it is expired.
func (tx *Tx[T]) Get(key string, ignoreExpired ...bool) (val *T, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := tx.db.get(key)
	if item == nil || (item.expired() && !ignore) {
		// The item does not exist or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return nil, ErrNotFound
	}
	return &item.val, nil
}

// Delete removes an item from the database based on the item's Key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx[T]) Delete(key string) (val *T, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if !tx.writable {
		return nil, ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return nil, ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&dbItem[T]{key: key})
	if item == nil {
		return nil, ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	if tx.wc.rbKeys == nil {
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}
	if tx.db.persistence.isActive {
		tx.wc.commitItems[key] = nil
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return nil, ErrNotFound
	}
	return &item.val, nil
}

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
func (tx *Tx[_]) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := time.Until(item.opts.exat)
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// PivotKV is a Key/value pair that is used to pivot a range of items.
// The Key is used to find the item in the database by key and as fallback if no value is given
// The Value is used to find the item in the database by value if an index is present
type PivotKV[T any] struct {
	K string
	V T
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx[T]) scan(desc, gt, lt bool, index string, start PivotKV[T], stop PivotKV[T],
	iterator func(key string, value T) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item *dbItem[T]) bool {
		dbi := item
		return iterator(dbi.key, dbi.val)
	}
	var tr tree.Store[*dbItem[T]]
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.indices[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *dbItem[T]
	if gt || lt {
		if index == "" {
			itemA = &dbItem[T]{key: start.K}
			itemB = &dbItem[T]{key: stop.K}
		} else {
			itemA = &dbItem[T]{val: start.V}
			itemB = &dbItem[T]{val: stop.V}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.iterCount++
		defer func() {
			tx.wc.iterCount--
		}()
	}
	if desc {
		if gt {
			if lt {
				tr.DescendRange(&itemA, &itemB, iter)
			} else {
				tr.DescendGT(&itemA, iter)
			}
		} else if lt {
			tr.DescendLTE(&itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(&itemA, &itemB, iter)
			} else {
				tr.AscendGTE(&itemA, iter)
			}
		} else if lt {
			tr.AscendLT(&itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx[T]) AscendKeys(pattern string,
	iterator func(key string, value T) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key string, value T) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", PivotKV[T]{K: min}, func(key string, value T) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx[T]) DescendKeys(pattern string,
	iterator func(key string, value T) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key string, value T) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", PivotKV[T]{K: max}, func(key string, value T) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) Ascend(index string,
	iterator func(key string, value T) bool) error {
	return tx.scan(false, false, false, index, PivotKV[T]{K: ""}, PivotKV[T]{K: ""}, iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) AscendGreaterOrEqual(index string, pivot PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(false, true, false, index, pivot, *new(PivotKV[T]), iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
// excluding the pivot
func (tx *Tx[T]) AscendLessThan(index string, lessThan PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(false, false, true, index, lessThan, *new(PivotKV[T]), iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
// including greaterOrEqual, excluding lessThan
func (tx *Tx[T]) AscendRange(index string, greaterOrEqual, lessThan PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) Descend(index string,
	iterator func(key string, value T) bool) error {
	return tx.scan(true, false, false, index, *new(PivotKV[T]), *new(PivotKV[T]), iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) DescendGreaterThan(index string, pivot PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(true, true, false, index, pivot, *new(PivotKV[T]), iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) DescendLessOrEqual(index string, pivot PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(true, false, true, index, pivot, *new(PivotKV[T]), iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) DescendRange(index string, lessOrEqual, greaterThan PivotKV[T],
	iterator func(key string, value T) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) AscendEqual(index string, pivot PivotKV[T],
	iterator func(key string, value T) bool) error {
	var err error
	var less func(a, b T) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key string, value T) bool {
		if less == nil {
			if key != pivot.K {
				return false
			}
		} else if less(pivot.V, value) {
			return false
		}
		return iterator(key, value)
	})
}

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the comparator function of the defined index.
// When an index is not provided, the results will be ordered by the item Key.
// An invalid index will return an error.
func (tx *Tx[T]) DescendEqual(index string, pivot PivotKV[T],
	iterator func(key string, value T) bool) error {
	var err error
	var less func(a, b T) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.DescendLessOrEqual(index, pivot, func(key string, value T) bool {
		if less == nil {
			if key != pivot.K {
				return false
			}
		} else if less(value, pivot.V) {
			return false
		}
		return iterator(key, value)
	})
}

// Len returns the number of items in the database
func (tx *Tx[T]) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexLen returns the number of items in the index
// if index identifier is empty the total Len of the DB will be returned
func (tx *Tx[T]) IndexLen(index string) (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	if index == "" {
		return tx.db.keys.Len(), nil
	}
	if _, ok := tx.db.indices[index]; !ok {
		return 0, ErrIndexNotFound
	}
	if tx.db.indices[index].btr == nil {
		return 0, ErrNotFound
	}
	return tx.db.indices[index].btr.Len(), nil
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
func (tx *Tx[T]) CreateIndex(name, pattern string,
	less ...func(a, b T) bool) error {
	return tx.createIndex(name, pattern, less, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx[T]) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b T) bool) error {
	return tx.createIndex(name, pattern, less, opts)
}

// createIndex is called by CreateIndex() and CreateIndexOptions()
func (tx *Tx[T]) createIndex(name string, pattern string,
	lessers []func(a, b T) bool,
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without identifier (preserved for keys tree)
		return ErrIndexExists
	}
	// check if index already exists
	if _, ok := tx.db.indices[name]; ok {
		return ErrIndexExists
	}
	// generate a less function
	less := combineComparators[T](lessers)
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// initialize new index
	idx := &index[T]{
		name:    name,
		pattern: pattern,
		less:    less,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	tx.db.indices[name] = idx
	if tx.wc.rbKeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use nil to indicate that the index should be removed upon
			// rollback.
			tx.wc.rollbackIndexes[name] = nil
		}
	}
	return nil
}

// DropIndex removes an index.
func (tx *Tx[T]) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.db.indices[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(tx.db.indices, name)
	if tx.wc.rbKeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate
			// that the index should be rebuilt upon rollback.
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// Indexes returns a list of index names.
func (tx *Tx[T]) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.indices))
	for name := range tx.db.indices {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
