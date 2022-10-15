package vastdb

import (
	"errors"
	"github.com/tidwall/btree"
)

var (
	// ErrItemNil is returned when an item is nil.
	ErrItemNil = errors.New("item is nil")

	// ErrKeyNil is returned when a key is nil.
	ErrKeyNil = errors.New("key is nil")
)

type GBTree[T any] struct {
	tr *btree.BTreeG[T]
}

func newGBtree[T any](less func(a, b T) bool) *GBTree[T] {
	return &GBTree[T]{tr: btree.NewBTreeGOptions[T](less, btree.Options{NoLocks: true})}
}

// ascend is a generic function to ascend the tree. (Internal)
func (gbt *GBTree[T]) ascend(pivot *T, iter func(item T) bool) {
	if pivot == nil {
		gbt.tr.Scan(iter)
	} else {
		gbt.tr.Ascend(*pivot, iter)
	}
}

// descend is a generic function to descend the tree from pivot. (Internal)
// if pivot is nil it iterates the complete tree in reverse direction
func (gbt *GBTree[T]) descend(pivot *T, iter func(item T) bool) {
	if pivot == nil {
		gbt.tr.Reverse(iter)
	} else {
		gbt.tr.Descend(*pivot, iter)
	}
}

// Set sets the item in the tree. If the item already exists, it will be replaced.
func (gbt *GBTree[T]) Set(item *T, hint *btree.PathHint) (prev *T, err error) {
	if item == nil {
		return nil, ErrItemNil
	}
	v, ok := gbt.tr.SetHint(*item, hint)
	if !ok {
		return nil, nil
	}
	return &v, nil
}

// Delete deletes the item from the tree by key. If the item does not exist, it will return nil.
// trows error if key is nil
// todo test
func (gbt *GBTree[T]) Delete(key *T) (prev *T, err error) {
	if key == nil {
		return nil, ErrKeyNil
	}
	v, ok := gbt.tr.DeleteHint(*key, nil)
	if !ok {
		return nil, nil
	}
	return &v, nil
}

// Get gets the item from the tree by key. If the item does not exist, it will return nil.
func (gbt *GBTree[T]) Get(key *T, hint *btree.PathHint) (value *T) {
	if key == nil {
		return nil
	}
	v, ok := gbt.tr.GetHint(*key, hint)
	if !ok {
		return nil
	}
	return &v
}

// Walk iterates over all items in the tree by the initial defined order.
func (gbt *GBTree[T]) Walk(iter func(items []T)) {
	gbt.tr.Walk(func(items []T) bool {
		iter(items)
		return true
	})
}

// Ascend iterates over all items in the tree by the initial defined order.
func (gbt *GBTree[T]) Ascend(iter func(item T) bool) {
	gbt.ascend(nil, iter)
}

// AscendLT iterates over all items in the tree less than the pivot.
func (gbt *GBTree[T]) AscendLT(lt *T, iter func(item T) bool) {
	if lt == nil {
		return
	}
	gbt.ascend(nil, func(item T) bool {
		return gbt.tr.Less(item, *lt) && iter(item)
	})
}

// AscendGTE iterates over all items in the tree greater than or equal to pivot.
func (gbt *GBTree[T]) AscendGTE(gte *T, iter func(item T) bool) {
	gbt.ascend(gte, iter)
}

// AscendRange iterates over all items in the tree greater than or equal to pivot and less than to pivot.
func (gbt *GBTree[T]) AscendRange(gte, lt *T, iter func(item T) bool) {
	if gte == nil && lt == nil {
		gbt.Ascend(iter)
		return
	}
	if gte == nil {
		gbt.AscendLT(lt, iter)
		return
	}
	if lt == nil {
		gbt.AscendGTE(gte, iter)
		return
	}
	gbt.ascend(gte, func(item T) bool {
		return gbt.tr.Less(item, *lt) && iter(item)
	})
}

// Descend iterates over all items in the tree in reverse order.
func (gbt *GBTree[T]) Descend(iter func(item T) bool) {
	gbt.descend(nil, iter)
}

// DescendLTE iterates over all items in the tree less than or equal to pivot.
func (gbt *GBTree[T]) DescendLTE(lte *T, iter func(item T) bool) {
	if lte == nil {
		return
	}
	gbt.descend(lte, iter)
}

// DescendGT iterates over all items in the tree greater than pivot.
func (gbt *GBTree[T]) DescendGT(gt *T, iter func(item T) bool) {
	gbt.descend(nil, func(item T) bool {
		return gbt.tr.Less(*gt, item) && iter(item)
	})
}

// DescendRange iterates over all items in the tree less than or equal to pivot and greater than or equal to pivot.
func (gbt *GBTree[T]) DescendRange(lte, gt *T, iter func(item T) bool) {
	if lte == nil && gt == nil {
		gbt.Descend(iter)
		return
	}
	if gt == nil {
		gbt.DescendLTE(lte, iter)
		return
	}
	gbt.descend(lte, func(item T) bool {
		return gbt.tr.Less(*gt, item) && iter(item)
	})
}

// Len returns the number of items in the tree.
func (gbt *GBTree[T]) Len() int {
	return gbt.tr.Len()
}

// Min returns the minimum item in the tree.
func (gbt *GBTree[T]) Min() *T {
	v, ok := gbt.tr.Min()
	if !ok {
		return nil
	}
	return &v
}

// Max returns the maximum item in the tree.
func (gbt *GBTree[T]) Max() *T {
	v, ok := gbt.tr.Max()
	if !ok {
		return nil
	}
	return &v
}
