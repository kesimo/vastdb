package tree

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

func NewGBtree[T any](less func(a, b T) bool) *GBTree[T] {
	return &GBTree[T]{tr: btree.NewBTreeGOptions[T](less, btree.Options{NoLocks: true})}
}

// ascend is a generic function to ascend the tree. (Internal)
func (gbt *GBTree[T]) ascend(pivot *T, iter func(item T) bool) {
	if iter == nil {
		return
	}
	if pivot == nil {
		gbt.tr.Scan(iter)
	} else {
		gbt.tr.Ascend(*pivot, iter)
	}
}

// descend is a generic function to descend the tree from pivot. (Internal)
// if pivot is nil it iterates the complete tree in reverse direction
func (gbt *GBTree[T]) descend(pivot *T, iter func(item T) bool) {
	if iter == nil {
		return
	}
	if pivot == nil {
		gbt.tr.Reverse(iter)
	} else {
		gbt.tr.Descend(*pivot, iter)
	}
}

// Set sets the item in the tree. If the item already exists, it will be replaced.
func (gbt *GBTree[T]) Set(item *T) (prev *T, err error) {
	if item == nil {
		return nil, ErrItemNil
	}
	v, ok := gbt.tr.SetHint(*item, nil)
	if !ok {
		return nil, nil
	}
	return &v, nil
}

// Delete deletes the item from the tree by key. If the item does not exist, it will return nil.
// trows error if key is nil
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
func (gbt *GBTree[T]) Get(key *T) (value *T, ok bool) {
	if key == nil {
		return nil, false
	}
	v, ok := gbt.tr.GetHint(*key, nil)
	if !ok {
		return nil, false
	}
	return &v, true
}

// Walk iterates over all items in the tree by the initial defined order.
func (gbt *GBTree[T]) Walk(iter func(items []T)) {
	if iter == nil {
		return
	}
	gbt.tr.Walk(func(items []T) bool {
		iter(items)
		return true
	})
}

// Ascend iterates over all items in the tree by the initial defined order.
func (gbt *GBTree[T]) Ascend(iter func(item T) bool) {
	if iter == nil {
		return
	}
	gbt.ascend(nil, iter)
}

// AscendLT iterates over all items in the tree less than the pivot.
func (gbt *GBTree[T]) AscendLT(lt *T, iter func(item T) bool) {
	if lt == nil || iter == nil {
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
// if iter is nil does nothing
// if gte is nil iterates from the beginning
// if lt is nil iterates to the end
// if both gte and lt are nil iterates the whole tree
func (gbt *GBTree[T]) AscendRange(gte, lt *T, iter func(item T) bool) {
	if iter == nil {
		return
	}
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
// if lte is nil does no iteration
func (gbt *GBTree[T]) DescendLTE(lte *T, iter func(item T) bool) {
	if lte == nil {
		return
	}
	gbt.descend(lte, iter)
}

// DescendGT iterates over all items in the tree greater than pivot.
// if iter is nil does nothing
// if gt is nil iterates from the beginning
func (gbt *GBTree[T]) DescendGT(gt *T, iter func(item T) bool) {
	if iter == nil {
		return
	}
	if gt == nil {
		gbt.Descend(iter)
		return
	}
	gbt.descend(nil, func(item T) bool {
		return gbt.tr.Less(*gt, item) && iter(item)
	})
}

// DescendRange iterates over all items in the tree less than or equal to pivot and greater than pivot.
// if lte is nil iterates until the end
// if gt is nil iterates from beginning
// if both lte and gt are nil iterates the whole tree
func (gbt *GBTree[T]) DescendRange(lte, gt *T, iter func(item T) bool) {
	if iter == nil {
		return
	}
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
func (gbt *GBTree[T]) Min() (item *T, ok bool) {
	v, ok := gbt.tr.Min()
	if !ok {
		return nil, false
	}
	return &v, true
}

// Max returns the maximum item in the tree.
func (gbt *GBTree[T]) Max() (item *T, ok bool) {
	v, ok := gbt.tr.Max()
	if !ok {
		return nil, false
	}
	return &v, true
}
