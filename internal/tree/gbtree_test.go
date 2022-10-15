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
	prev, err := tree.Set(&item, nil)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
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
		tree.Set(&item, nil)
	}
	if tree.Len() != 10 {
		t.Errorf("Tree should have 10 items, got %v", tree.Len())
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
	prev, err := tree.Set(&item, nil)
	if err != nil {
		t.Errorf("Error setting item: %v", err)
	}
	if prev != nil {
		t.Errorf("Previous item should be nil")
	}
	len := tree.Len()
	if len != 1 {
		t.Errorf("Tree length should be 1, got %v", len)
	}
	itemGet := &dbItem[mockTestTree]{
		key: "hello",
	}
	fromTree, _ := tree.Get(&itemGet, nil)
	if fromTree == nil {
		t.Errorf("Item should not be nil")
	}
	if (*fromTree).val != item.val {
		t.Errorf("Item values should be equal")
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
	_, err := tree.Set(&item, nil)
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
		tree.Set(&item, nil)
	}
	tree.Walk(func(items []*dbItem[mockTestTree]) {
		for idx, item := range items {
			if item.key != "hello"+strconv.Itoa(idx) {
				t.Errorf("Items should be in order")
			}
		}
	})
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
		tree.Set(&item, nil)
	}
	curr := 0
	tree.Ascend(func(item *dbItem[mockTestTree]) bool {
		if item.key != "hello"+strconv.Itoa(curr) {
			t.Errorf("Items should be in order")
		}
		curr++
		return true
	})
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
		tree.Set(&item, nil)
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
}
