package tree

import (
	"strconv"
	"testing"
)

type dbItem[T any] struct {
	key     string // the key used for default btree sorting
	val     T      // generic value
	opts    any    // optional meta information
	keyless bool   // keyless item for scanning
}

type mockTestTree struct {
	Key string
	Num int
	sec int
}

func testTreeLess(a, b *dbItem[mockTestTree]) bool {
	return a.key < b.key
}

func testCreateGBTree() *GBTree[*dbItem[mockTestTree]] {
	return NewGBtree[*dbItem[mockTestTree]](testTreeLess)
}

func TestGBTree_New(t *testing.T) {
	gbt := testCreateGBTree()
	if gbt == nil {
		t.Error("gbt is nil")
	}
	//test empty tree comparator
	_ = NewGBtree[*dbItem[mockTestTree]](nil)
	//test empty tree type
	_ = NewGBtree[any](nil)
}

func TestGBTree_Set(t *testing.T) {
	tree := testCreateGBTree()
	item := &dbItem[mockTestTree]{
		key: "hello",
		val: mockTestTree{
			Key: "key1",
			Num: 10,
			sec: 15,
		},
		opts: nil,
	}
	prev, err := tree.Set(&item)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
	}
	//set nil item
	prev, err = tree.Set(nil)
	if err == nil {
		t.Errorf("Error setting nil item should not be nil")
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
	}
	//overwrite existing item and check previous
	item = &dbItem[mockTestTree]{
		key: "hello",
		val: mockTestTree{
			Key: "key1",
			Num: 12,
			sec: 15,
		},
		opts: nil,
	}
	prev, err = tree.Set(&item)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}
	if prev == nil {
		t.Errorf("Previous item should not be nil")
	}
	if (*prev).val.Num != 10 {
		t.Errorf("Previous item should have Num=10")
	}
}

func TestGBTree_Len(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	if tree.Len() != 10 {
		t.Errorf("Tree should have 10 items, got %v", tree.Len())
	}
	//check len of empty tree
	treeEmpty := testCreateGBTree()
	if treeEmpty.Len() != 0 {
		t.Errorf("Empty tree should have 0 items, got %v", treeEmpty.Len())
	}
}

func TestGBTree_Get(t *testing.T) {
	tree := testCreateGBTree()
	item := &dbItem[mockTestTree]{
		key: "hello",
		val: mockTestTree{
			Key: "key1",
			Num: 10,
			sec: 15,
		},
		opts: nil,
	}
	prev, err := tree.Set(&item)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
	}
	length := tree.Len()
	if length != 1 {
		t.Errorf("Tree length should be 1, got %v", length)
	}
	itemGet := &dbItem[mockTestTree]{
		key: "hello",
	}
	fromTree, _ := tree.Get(&itemGet)
	if fromTree == nil {
		t.Errorf("Item should not be nil")
	}
	if (*fromTree).val != item.val {
		t.Errorf("Item values should be equal")
	}
	//test get for nil item
	missing, ok := tree.Get(nil)
	if ok {
		t.Errorf("should not be okay, if key for Get is nil")
	}
	if missing != nil {
		t.Errorf("missing should ne nil, if key for Get is nil")
	}
	//test get for item not stored
	itemNotStored := &dbItem[mockTestTree]{key: "notAvailable"}
	notStored, ok := tree.Get(&itemNotStored)
	if ok {
		t.Errorf("should no be okay if no item for key found")
	}
	if notStored != nil {
		t.Errorf("Item recived from Get should be nil if not found")
	}

}

func TestGBTree_Delete(t *testing.T) {
	tree := testCreateGBTree()
	item := &dbItem[mockTestTree]{
		key: "hello",
		val: mockTestTree{
			Key: "key1",
			Num: 10,
			sec: 15,
		},
	}
	_, err := tree.Set(&item)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}

	// delete item
	itemToDel := &dbItem[mockTestTree]{
		key: "hello",
	}
	deleted, err := tree.Delete(&itemToDel)
	if err != nil {
		t.Errorf("Error deleting item: %v", err)
	}
	if deleted == nil {
		t.Errorf("Deleted item should not be nil")
	}
	if (*deleted).val != item.val {
		t.Errorf("Deleted item values should be equal")
	}
	//test delete item that is not in store
	itemNotStored := &dbItem[mockTestTree]{
		key: "notAvailable",
	}
	prev, err := tree.Delete(&itemNotStored)
	if err != nil {
		t.Errorf("Error should not occur if item is not in store")
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
	}
	//test delete for nil item
	prevNil, err := tree.Delete(nil)
	if err == nil {
		t.Errorf("Error should occur if item to delete is nil")
	}
	if prevNil != nil {
		t.Errorf("Previous item should be nil")
	}

}

func TestGBTree_Walk(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	tree.Walk(func(items []*dbItem[mockTestTree]) {
		for idx, item := range items {
			if item.key != "hello"+strconv.Itoa(idx) {
				t.Errorf("Items should be in order")
			}
		}
	})
	// walk empty tree
	treeEmpty := testCreateGBTree()
	treeEmpty.Walk(func(items []*dbItem[mockTestTree]) {
		t.Errorf("Walk should not be called for empty tree")
	})
	// walk with nil callback
	tree.Walk(nil)
}

func TestGBTree_Ascend(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	curr := 0
	tree.Ascend(func(item *dbItem[mockTestTree]) bool {
		if item.key != "hello"+strconv.Itoa(curr) {
			t.Errorf("Items should be in order")
		}
		curr++
		return true
	})
	if curr != 10 {
		t.Errorf("All items should be iterated")
	}
	// test ascend with nil callback
	tree.Ascend(nil)
}

func TestGBTree_AscendLT(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	lt := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.AscendLT(&lt, func(item *dbItem[mockTestTree]) bool {
		if item.key >= "hello5" {
			t.Errorf("Items should be less than hello5, got %v", item.key)
		}
		curr++
		return true
	})
	if curr != 5 {
		t.Errorf("Should iterate 5 times, got %v", curr)
	}
	// test ascendLT with nil callback
	tree.AscendLT(&lt, nil)
	// test with nil pivot
	tree.AscendLT(nil, func(item *dbItem[mockTestTree]) bool {
		t.Errorf("Callback should not be called for nil pivot")
		return true
	})
}

func TestGBTree_AscendGTE(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	gte := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.AscendGTE(&gte, func(item *dbItem[mockTestTree]) bool {
		if item.key < "hello5" {
			t.Errorf("Items should be greater than hello5, got %v", item.key)
		}
		curr++
		return true
	})
	if curr != 5 {
		t.Errorf("Should iterate 5 times, got %v", curr)
	}
	// test ascendGTE with nil callback
	tree.AscendGTE(&gte, nil)
	// test with nil pivot
	gteIter := 0
	tree.AscendGTE(nil, func(item *dbItem[mockTestTree]) bool {
		gteIter++
		return true
	})
	if gteIter != 10 {
		t.Errorf("Should iterate all items if gte pivot is nil, got %v", gteIter)
	}
}

func TestGBTree_AscendRange(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	lt := &dbItem[mockTestTree]{
		key: "hello9",
	}
	gte := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.AscendRange(&gte, &lt, func(item *dbItem[mockTestTree]) bool {
		if item.key < "hello5" || item.key >= "hello9" {
			t.Errorf("Items should be in range, got %v", item.key)
		}
		if item.key != "hello"+strconv.Itoa(curr+5) {
			t.Errorf("Items should be in order")
		}
		curr++
		return true
	})
	if curr != 4 {
		t.Errorf("Should iterate 4 times, got %v", curr)
	}
	// test ascendRange with nil callback
	tree.AscendRange(&gte, &lt, nil)
	// test with nil pivot gte
	ltIter := 0
	tree.AscendRange(nil, &lt, func(item *dbItem[mockTestTree]) bool {
		ltIter++
		return true
	})
	if ltIter != 9 {
		t.Errorf("Should iterate all items until lt if gte pivot is nil, got %v", ltIter)
	}
	// test with nil pivot lt
	gteIter := 0
	tree.AscendRange(&gte, nil, func(item *dbItem[mockTestTree]) bool {
		gteIter++
		return true
	})
	if gteIter != 5 {
		t.Errorf("Should iterate all items from gte if lt pivot is nil, got %v", gteIter)
	}
	// check if both are nil
	bNilIter := 0
	tree.AscendRange(nil, nil, func(item *dbItem[mockTestTree]) bool {
		bNilIter++
		return true
	})
	if bNilIter != 10 {
		t.Errorf("Should iterate all items if both pivots are nil, got %v", bNilIter)
	}
	// check if gte is greater than lt
	revIter := 0
	tree.AscendRange(&lt, &gte, func(item *dbItem[mockTestTree]) bool {
		revIter++
		return true
	})
	if revIter != 0 {
		t.Errorf("Should iterate 0 times if gte is greater than lt, got %v", revIter)
	}
}

func TestGBTree_Descend(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	curr := 9
	tree.Descend(func(item *dbItem[mockTestTree]) bool {
		if item.key != "hello"+strconv.Itoa(curr) {
			t.Errorf("Items should be in order")
		}
		curr--
		return true
	})
	if curr != -1 {
		t.Errorf("Should iterate 10 times, got %v", 9-curr)
	}
	// test descend with nil callback
	tree.Descend(nil)
}

func TestGBTree_DescendGT(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	gt := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.DescendGT(&gt, func(item *dbItem[mockTestTree]) bool {
		if item.key <= "hello5" {
			t.Errorf("Items should be greater than hello5, got %v", item.key)
		}
		curr++
		return true
	})
	if curr != 4 {
		t.Errorf("Should iterate 4 times, got %v", curr)
	}
	// test descendGT with nil callback
	tree.DescendGT(&gt, nil)
	// test with nil pivot
	gtIter := 0
	tree.DescendGT(nil, func(item *dbItem[mockTestTree]) bool {
		gtIter++
		return true
	})
	if gtIter != 10 {
		t.Errorf("Should iterate all items if gt pivot is nil, got %v", gtIter)
	}
}

func TestGBTree_DescendLTE(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	lte := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.DescendLTE(&lte, func(item *dbItem[mockTestTree]) bool {
		if item.key > "hello5" {
			t.Errorf("Item should be less than pivot")
		}
		curr++
		return true
	})
	if curr != 6 {
		t.Errorf("Should iterate 6 times, got %v", curr)
	}
	// test descendLTE with nil callback
	tree.DescendLTE(&lte, nil)
	// test with nil pivot
	tree.DescendLTE(nil, func(item *dbItem[mockTestTree]) bool {
		t.Errorf("Should not iterate if pivot is nil")
		return true
	})
}

func TestGBTree_DescendRange(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	lte := &dbItem[mockTestTree]{
		key: "hello9",
	}
	gt := &dbItem[mockTestTree]{
		key: "hello5",
	}
	curr := 0
	tree.DescendRange(&lte, &gt, func(item *dbItem[mockTestTree]) bool {
		if item.key > "hello9" || item.key <= "hello5" {

			t.Errorf("Items should be in range gt < i <= lte")
		}
		if item.key != "hello"+strconv.Itoa(9-curr) {
			t.Errorf("Items should be in order")
		}
		curr++
		return true
	})
	if curr != 4 {
		t.Errorf("Should iterate 4 times, got %v", curr)
	}
	// test descendRange with nil callback
	tree.DescendRange(&lte, &gt, nil)
	// test with nil lte pivot
	lteIter := 0
	tree.DescendRange(nil, &gt, func(item *dbItem[mockTestTree]) bool {
		t.Logf("item: %v", item.key)
		lteIter++
		return true
	})
	if lteIter != 4 {
		t.Errorf("Should iterate 4 times if lte pivot is nil, got %v", lteIter)
	}
	// test with nil gt pivot
	gtIter := 0
	tree.DescendRange(&lte, nil, func(item *dbItem[mockTestTree]) bool {
		gtIter++
		return true
	})
	if gtIter != 10 {
		t.Errorf("Should iterate all items if gt pivot is nil, got %v", gtIter)
	}
	// test with nil lte and gt pivot
	bothNilIter := 0
	tree.DescendRange(nil, nil, func(item *dbItem[mockTestTree]) bool {
		bothNilIter++
		return true
	})
	if bothNilIter != 10 {
		t.Errorf("Should iterate all items if both pivot is nil, got %v", bothNilIter)
	}

}

func TestGBTree_Max(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	max, ok := tree.Max()
	if !ok {
		t.Errorf("failed to get Max item")
	}
	if (*max).key != "hello9" {
		t.Errorf("wrong max item")
	}
	// test with empty tree
	emptyTree := testCreateGBTree()
	_, ok = emptyTree.Max()
	if ok {
		t.Errorf("should return false when tree is empty")
	}
}

func TestGBTree_Min(t *testing.T) {
	tree := testCreateGBTree()
	for i := 0; i < 10; i++ {
		item := &dbItem[mockTestTree]{
			key: "hello" + strconv.Itoa(i),
			val: mockTestTree{
				Key: "key1",
				Num: 10,
				sec: 15,
			},
			opts: nil,
		}
		tree.Set(&item)
	}
	min, ok := tree.Min()
	if !ok {
		t.Errorf("failed to get Max item")
	}
	if (*min).key != "hello0" {
		t.Errorf("wrong max item")
	}
	// test with empty tree
	emptyTree := testCreateGBTree()
	_, ok = emptyTree.Min()
	if ok {
		t.Errorf("should return false when tree is empty")
	}
}
