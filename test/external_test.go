package test

import (
	"github.com/kesimo/vastdb"
	"testing"
)

type mockExt struct {
	Key        string
	Val        string
	Additional int
}

func testSetupDB() *vastdb.DB[mockExt] {
	db, err := vastdb.Open[mockExt](":memory:")
	if err != nil {
		panic(err)
	}
	return db
}

func TestSetupIndex(t *testing.T) {
	db := testSetupDB()
	err := db.CreateIndex("test", "*", func(a, b mockExt) bool {
		return a.Additional < b.Additional
	})
	if err != nil {
		t.Errorf("error creating index: %v", err)
	}
}

func TestCRD(t *testing.T) {
	db := testSetupDB()
	//test set item
	_, err := db.Set("key1", mockExt{
		Key:        "key1",
		Val:        "val1",
		Additional: 1,
	})
	if err != nil {
		t.Errorf("error setting item: %v", err)
	}

	//test get item
	item, err := db.Get("key1")
	if err != nil {
		t.Errorf("error getting item: %v", err)
	}
	if item.Key != "key1" {
		t.Errorf("error getting item: %v", err)
	}
	//delete item
	_, err = db.Del("key1")
	if err != nil {
		t.Errorf("error deleting item: %v", err)
	}
	//check if item changed:
	if item.Key != "key1" {
		t.Errorf("error getting item: %v", err)
	}

	//update item
	_, err = db.Set("key1", mockExt{
		Key: "key2",
		Val: "val2",
	})
	if err != nil {
		t.Errorf("error setting item: %v", err)
	}
	//check if item changed:
	if item.Key != "key1" {
		t.Errorf("item pointer changed: %v", err)
	}
}

func TestTx_Update(t *testing.T) {
	db := testSetupDB()
	//set item
	_, err := db.Set("key1", mockExt{
		Key: "key1",
		Val: "val1",
	})
	if err != nil {
		t.Errorf("error setting item: %v", err)
	}
	//test update item
	err = db.Update(func(tx *vastdb.Tx[mockExt]) error {
		// get item from tx
		item, err := tx.Get("key1")
		if err != nil {
			return err
		}
		// update item
		item.Val = "val2"
		// set item
		_, _, err = tx.Set("key1", *item, nil)
		return nil
	})
	if err != nil {
		t.Errorf("error updating item: %v", err)
	}
}
