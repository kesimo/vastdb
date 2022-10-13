package vastdb

import (
	"github.com/tidwall/btree"
)

//// Generic btree wrappers
func gBLT[T any](tr *btree.BTreeG[T], a, b T) bool { return tr.Less(a, b) }
func gBGT[T any](tr *btree.BTreeG[T], a, b T) bool { return tr.Less(b, a) }

func gAscend[T any](tr *btree.BTreeG[T], pivot *T, iter func(item T) bool) {
	if pivot == nil {
		tr.Scan(iter)
	} else {
		tr.Ascend(*pivot, iter)
	}
}

func gDescend[T any](tr *btree.BTreeG[T], pivot *T, iter func(item T) bool) {
	if pivot == nil {
		tr.Reverse(iter)
	} else {
		tr.Descend(*pivot, iter)
	}
}

func gBtreeSetHint[T any](tr *btree.BTreeG[T], item *T, hint *btree.PathHint) (prev *T) {
	if item == nil {
		panic("nil item") // todo handle panic
	}
	v, ok := tr.SetHint(*item, hint)
	if !ok {
		return nil
	}
	return &v
}

func gBtreeDeleteHint[T any](tr *btree.BTreeG[T], key *T) (prev *T) {
	if key == nil {
		return nil
	}
	v, ok := tr.DeleteHint(*key, nil)
	if !ok {
		return nil
	}
	return &v
}

func gBtreeGetHint[T any](tr *btree.BTreeG[T], key *T, hint *btree.PathHint) (value *T) {
	if key == nil {
		return nil
	}
	v, ok := tr.GetHint(*key, hint)
	if !ok {
		return nil
	}
	return &v
}

func gBtreeWalk[T any](tr *btree.BTreeG[T], iter func(items []T)) {
	tr.Walk(func(items []T) bool {
		iter(items)
		return true
	})
}

// Ascend generic

func gBtreeAscend[T any](tr *btree.BTreeG[T], iter func(item T) bool) {
	gAscend(tr, nil, iter)
}

func gBtreeAscendLessThan[T any](tr *btree.BTreeG[T], pivot *T,
	iter func(item T) bool,
) {
	if pivot == nil {
		return
	}
	gAscend(tr, nil, func(item T) bool {
		return gBLT(tr, item, *pivot) && iter(item)
	})
}

func gBtreeAscendGreaterOrEqual[T any](tr *btree.BTreeG[T], pivot *T,
	iter func(item T) bool,
) {
	gAscend(tr, pivot, iter)
}

func gBtreeAscendRange[T any](tr *btree.BTreeG[T], greaterOrEqual, lessThan *T,
	iter func(item T) bool,
) {
	if greaterOrEqual == nil && lessThan == nil {
		gBtreeAscend(tr, iter) //todo check if required
		return
	}
	gAscend(tr, greaterOrEqual, func(item T) bool {
		return gBLT(tr, item, *lessThan) && iter(item)
	})
}

// Descend generic

func gBtreeDescend[T any](tr *btree.BTreeG[T], iter func(item T) bool) {
	gDescend(tr, nil, iter)
}

func gBtreeDescendGreaterThan[T any](tr *btree.BTreeG[T], pivot *T,
	iter func(item T) bool,
) {
	gDescend(tr, pivot, func(item T) bool {
		return gBGT(tr, item, *pivot) && iter(item)
	})
}

func gBtreeDescendRange[T any](tr *btree.BTreeG[T], lessOrEqual *T, greaterThan *T,
	iter func(item T) bool,
) {
	if lessOrEqual == nil && greaterThan == nil {
		gBtreeDescend(tr, iter) //todo check if required
		return
	}
	tr.Descend(*lessOrEqual, func(item T) bool {
		return gBGT(tr, item, *greaterThan) && iter(item)
	})
}

func gBtreeDescendLessOrEqual[T any](tr *btree.BTreeG[T], pivot *T,
	iter func(item T) bool,
) {
	if pivot == nil {
		return
	}
	tr.Descend(*pivot, iter)
}

func gBtreeNew[T any](less func(a, b T) bool) *btree.BTreeG[T] {
	// Using NewNonConcurrent because we're managing our own locks.
	return btree.NewBTreeGOptions[T](less, btree.Options{NoLocks: true})
}
