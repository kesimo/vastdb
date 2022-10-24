// This file is part of the vastDB project.
// Last modified : Kevin Eder
// Creation date: 10.10.2022
// License: MIT
// Use of this source code is governed by a MIT license that can be found in the LICENSE file
// at https://github.com/kesimo/vastdb/blob/main/LICENSE

package vastdb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type mock struct {
	Key       string `json:"Key"`
	Workspace string `json:"Workspace"`
	Num       int    `json:"Num"`
}

// wait until the next second with zero milliseconds and nanoseconds reached
func testWaitZeroSecond() {
	now := time.Now()
	for now.Nanosecond() > 0 || now.Nanosecond() > 0 {
		now = time.Now()
	}
}

func testOpen(t testing.TB) *DB[mock] {
	if err := os.RemoveAll("data.db"); err != nil {
		t.Fatal(err)
	}
	return testReOpen(t, nil)
}

func testReOpen(t testing.TB, db *DB[mock]) *DB[mock] {
	return testReOpenDelay(t, db, 0)
}

func testReOpenDelay(t testing.TB, db *DB[mock], dur time.Duration) *DB[mock] {
	if db != nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(dur)
	db, err := Open("data.db", mock{})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testClose(db *DB[mock]) {
	_ = db.Close()
	_ = os.RemoveAll("data.db")
}

func TestDB_Open(t *testing.T) {
	// test using empty struct
	db, err := Open("", struct{}{})
	if err == nil {
		t.Fatal("should not be able to open a database with an" +
			" struct that does not export any fields")
	}
	if db != nil {
		t.Fatal("expecting nil, got DB object")
	}
	// test using interface type
	_, err = Open("", interface{}(nil))
	if err == nil {
		t.Fatal("should not be able to open a database with an" +
			" interface type")
	}
	// test using pointer as value
	_, err = Open("", &struct{ Key string }{Key: "test"})
	if err != nil {
		t.Fatalf("should be able to open a database with a pointer as value: %v", err)
	}
	// test using slice as value
	_, err = Open("", []struct{ Key string }{{Key: "test"}})
	if err != nil {
		t.Fatalf("should be able to open a database with a slice as value: %v", err)
	}
	// test using map as value
	_, err = Open("", map[string]struct{ Key string }{"test": {Key: "test"}})
	if err != nil {
		t.Fatalf("should be able to open a database with a map as value: %v", err)
	}
	// test using interface as value -> should fail
	_, err = Open("", interface{}(struct{ Key string }{Key: "test"}))
	if err == nil {
		t.Fatal("should not be able to open a database with an interface as value:")
	}
	// test using struct with pointers as value
	_, err = Open("", struct{ Key *string }{Key: new(string)})
	if err != nil {
		t.Fatalf("should be able to open a database with a struct with pointers as value: %v", err)
	}
	// test using slice with pointers as value
	_, err = Open("", []*mock{})
	if err != nil {
		t.Fatalf("should be able to open a database with a slice with pointers as value: %v", err)
	}
	// test using multiple values
	_, err = Open("", struct{ Key string }{Key: "test"}, struct{ Key string }{Key: "test"})
	if err != nil {
		t.Fatalf("should be able to open a database with multiple values: %v", err)
	}
}

func TestDB_BackgroundOperations(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	for i := 0; i < 1000; i++ {
		if err := db.Update(func(tx *Tx[mock]) error {
			for j := 0; j < 200; j++ {
				if _, _, err := tx.Set(fmt.Sprintf("test%d", j), mock{
					Key:       "test" + strconv.Itoa(j),
					Workspace: "ws2",
					Num:       50,
				}, nil); err != nil {
					return err
				}
			}
			if _, _, err := tx.Set("hi", mock{
				Key:       "hi",
				Workspace: "ws1",
				Num:       50,
			}, &SetOptions{Expires: true, TTL: time.Second / 2}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	err := db.View(func(tx *Tx[mock]) error {
		var err error
		n, err = tx.Len()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 201 {
		t.Fatalf("expecting '%v', got '%v'", 201, n)
	}
	time.Sleep(time.Millisecond * 1500)
	db = testReOpen(t, db)
	defer testClose(db)
	n = 0
	err = db.View(func(tx *Tx[mock]) error {
		var err error
		n, err = tx.Len()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("expecting '%v', got '%v'", 200, n)
	}
}

func TestDB_SaveLoad(t *testing.T) {
	db, _ := Open(":memory:", mock{})
	defer db.Close()
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 20; i++ {
			_, _, err := tx.Set(fmt.Sprintf("Key:%d", i), mock{
				Key:       fmt.Sprintf("Key:%d", i),
				Workspace: "ws1",
				Num:       50,
			}, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.Close()
		err := os.RemoveAll("temp.db")
		if err != nil {
			t.Fatal(err)
		}
	}()
	if err := db.Snapshot(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, _ = Open(":memory:", mock{})
	defer db.Close()
	f, err = os.Open("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := db.Load(f); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[mock]) error {
		for i := 0; i < 20; i++ {
			ex := &mock{
				Key:       fmt.Sprintf("Key:%d", i),
				Workspace: "ws1",
				Num:       50,
			}
			val, err := tx.Get(fmt.Sprintf("Key:%d", i))
			if err != nil {
				return err
			}
			if ex != val {
				if ex.Key != val.Key || ex.Workspace != val.Workspace || ex.Num != val.Num {
					t.Fatalf("expected %v, got %v", ex, val)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_SaveLoadExceedBuffer(t *testing.T) {
	db, _ := Open("", mock{})
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 50000; i++ {
			_, _, err := tx.Set(fmt.Sprintf("Key:%d", i), mock{
				Key:       fmt.Sprintf("Key:%d", i),
				Workspace: "ws1",
				Num:       5034242342,
			}, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.Close()
		err := os.RemoveAll("temp.db")
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Snapshot(f)
	if err != nil {
		t.Errorf("error saving db: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// test load in persistent db
	db2, _ := Open("temp2.db", mock{})
	defer func() {
		db2.Close()
		err := os.RemoveAll("temp2.db")
		if err != nil {
			t.Fatal(err)
		}
	}()
	f2, err := os.Open("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	err2 := db2.Load(f2)
	if err2 == nil {
		t.Errorf("expecting error while loading into persistent db, got nil")
	}

	testClose(db)
	db3, _ := Open(":memory:", mock{})
	defer testClose(db3)
	// read the file back in
	f3, err := os.Open("temp.db")
	err = db3.Load(f3)
	if err != nil {
		t.Errorf("error loading db: %v", err)
	}
	if err := db3.View(func(tx *Tx[mock]) error {
		for i := 0; i < 50000; i++ {
			ex := &mock{
				Key:       fmt.Sprintf("Key:%d", i),
				Workspace: "ws1",
				Num:       5034242342,
			}
			val, err := tx.Get(fmt.Sprintf("Key:%d", i))
			if err != nil {
				return err
			}
			if ex != val {
				if ex.Key != val.Key || ex.Workspace != val.Workspace || ex.Num != val.Num {
					t.Fatalf("expected %v, got %v", ex, val)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_ErrAlreadyClosed(t *testing.T) {
	db, _ := Open(":memory:", mock{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatalf("expecting '%v', got '%v'", ErrDatabaseClosed, err)
	}
}

func TestDB_ErrConfigClosed(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	_ = db.Close()
	var config Config[mock]
	if err := db.ReadConfig(&config); err != ErrDatabaseClosed {
		t.Fatal("expecting database closed error")
	}
	if err := db.SetConfig(config); err != ErrDatabaseClosed {
		t.Fatal("expecting database closed error")
	}
}

func TestDB_ErrShrinkInProcess(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10000; i++ {
			_, _, err := tx.Set(fmt.Sprintf("%d", i), mock{
				Key: fmt.Sprintf("%d", i),
			}, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 250; i < 350; i++ {
			_, err := tx.Delete(fmt.Sprintf("%d", i))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = db.Shrink()
	}()
	go func() {
		defer wg.Done()
		err2 = db.Shrink()
	}()
	wg.Wait()
	//println(123)
	//fmt.Printf("%v\n%v\n", err1, err2)
	if err1 != ErrShrinkInProcess && err2 != ErrShrinkInProcess {
		t.Fatal("expecting a shrink in process error")
	}
	db = testReOpen(t, db)
	defer testClose(db)
	if err := db.View(func(tx *Tx[mock]) error {
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != 9900 {
			t.Fatal("expecting 9900 items")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// test opening a folder.
func TestDB_ErrOpenFolder(t *testing.T) {
	if err := os.RemoveAll("dir.tmp"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("dir.tmp", 0700); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll("dir.tmp") }()
	db, err := Open[mock]("dir.tmp")
	if err == nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("opening a directory should not be allowed")
	}
}

func TestDB_len(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 20; i++ {
			_, _, err := tx.Set(fmt.Sprintf("Key:%d", i), mock{
				Key:       fmt.Sprintf("Key:%d", i),
				Workspace: "ws1",
				Num:       50,
			}, nil)
			if err != nil {
				t.Errorf("error setting key: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Errorf("error updating db: %v", err)
	}
	// test get len by using view
	n := 0
	err := db.View(func(tx *Tx[mock]) error {
		var err error
		n, err = tx.Len()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("expecting (tx.Len()) '%v', got '%v'", 20, n)
	}
	// test get len by using db.Len()
	n, err = db.Len()
	if err != nil {
		t.Errorf("error getting len: %v", err)
	}
	if n != 20 {
		t.Fatalf("expecting (db.Len()) '%v', got '%v'", 20, n)
	}
}

func TestDB_Set(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	//test default functionality
	prev, err := db.Set("key", mock{
		Key: "key",
		Num: 50,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prev != nil {
		t.Fatalf("expecting nil, got %v", prev)
	}
	//test set empty key
	prev, err = db.Set("", mock{Key: "key-empty"}, nil)
	if err == nil {
		t.Fatalf("expecting error, got nil")
	}
}

func TestDB_SetWithTTL(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	//test set ttl
	_, err := db.Set("key-ttl", mock{Key: "key-ttl"}, &SetOptions{TTL: time.Second * 2, Expires: true})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	item, err := db.Get("key-ttl")
	if err == nil {
		t.Errorf("expecting error, got nil")
	}
	if item != nil {
		t.Errorf("expecting nil, got %v", item)
	}
}

func TestDB_OnExpired(t *testing.T) {
	testWaitZeroSecond()
	db := testOpen(t)
	defer testClose(db)
	// apply expires function
	db.SetConfig(Config[mock]{
		OnExpired: func(keys []string) {
			if len(keys) != 10 {
				t.Errorf("OnExpired: expecting 10 keys, got %v", len(keys))
			}
		},
	})
	//test set ttl
	for i := 0; i < 10; i++ {
		_, err := db.Set(fmt.Sprintf("key-ttl-%d", i), mock{Key: fmt.Sprintf("key-ttl-%d", i)}, &SetOptions{TTL: 0, Expires: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Millisecond * 4000)
	l, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	if l != 10 {
		t.Errorf("Should not delete items when overwriting onExpires Function: expecting 10 items, got %v", l)
	}
}

// WARNING: this test is not deterministic, it may fail sometimes, b.c. it depends on the GC timing.
func TestDB_OnExpiredSync(t *testing.T) {
	testWaitZeroSecond()
	db := testOpen(t)
	defer testClose(db)
	onExpSyncCount := 0
	mu := sync.Mutex{}
	// apply expires function
	db.SetConfig(Config[mock]{
		OnExpiredSync: func(key string, value mock, tx *Tx[mock]) error {
			mu.Lock()
			defer mu.Unlock()
			onExpSyncCount = onExpSyncCount % 10
			calcKey := fmt.Sprintf("key-ttl-%d", onExpSyncCount)
			if key != calcKey || value.Key != calcKey {
				t.Errorf("OnExpiredSync: expecting %s, got %v (val.key=%v)", calcKey, key, value.Key)
			}
			onExpSyncCount++

			return nil
		},
	})
	//test set ttl
	for i := 0; i < 10; i++ {
		_, err := db.Set(fmt.Sprintf("key-ttl-%d", i), mock{Key: fmt.Sprintf("key-ttl-%d", i)}, &SetOptions{TTL: time.Second * 2, Expires: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Millisecond * 2500)
}

func TestDB_SetGetPreviousItem(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	//test set previous item
	prev, err := db.Set("key", mock{Key: "first"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prev != nil {
		t.Fatalf("expecting nil, got %v", prev)
	}
	prev, err = db.Set("key", mock{Key: "updated"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prev == nil {
		t.Fatalf("expecting not nil, got %v", prev)
	}
	if (*prev).Key != "first" {
		t.Fatalf("expecting 'first', got %v", prev)
	}
}

func TestDB_Get(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	//test get empty key
	_, errEmptyKey := db.Get("")
	if errEmptyKey == nil {
		t.Fatalf("expecting error, got nil")
	}
	//test get non-existent key
	_, errNonExistingKey := db.Get("key")
	if errNonExistingKey == nil {
		t.Fatalf("expecting error, got nil")
	}
	if errNonExistingKey != ErrNotFound {
		t.Fatalf("expecting NotFound error, got %v", errNonExistingKey)
	}
	//test get key
	_, errSet := db.Set("key1", mock{
		Key: "key",
		Num: 50,
	}, nil)
	if errSet != nil {
		t.Fatal(errSet)
	}
	item, err := db.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	if item.Key != "key" || item.Num != 50 {
		t.Fatalf("expecting %v, got %v", mock{
			Key: "key",
			Num: 50,
		}, item)
	}
}

func TestDB_Del(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	//test del empty key
	_, errEmptyKey := db.Del("")
	if errEmptyKey == nil {
		t.Fatalf("expecting error, got nil")
	}
	//test del non-existent key
	_, errNonExistingKey := db.Del("key")
	if errNonExistingKey == nil {
		t.Fatalf("expecting error, got nil")
	}
	if errNonExistingKey != ErrNotFound {
		t.Fatalf("expecting NotFound error, got %v", errNonExistingKey)
	}
	//test del key if index is set
	err := db.CreateIndex("num", "*", func(a, b mock) bool { return a.Num < b.Num })
	if err != nil {
		t.Fatal(err)
	}
	_, errSet := db.Set("key1", mock{
		Key: "key",
		Num: 50,
	}, nil)
	if errSet != nil {
		t.Fatal(errSet)
	}
	_, err = db.Del("key1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Get("key1")
	if err != ErrNotFound {
		t.Fatalf("expecting NotFound error, got %v", err)
	}
}

func TestMutatingIterator(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	idxFn := func(a mock, b mock) bool {
		return a.Num < b.Num
	}
	count := 1000
	if err := db.CreateIndex("ages", "user:*:age", idxFn); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := db.Update(func(tx *Tx[mock]) error {
			for j := 0; j < count; j++ {
				key := fmt.Sprintf("user:%d:age", j)
				val := mock{
					Key:       key,
					Workspace: "ws1",
					Num:       rand.Intn(100),
				}
				if _, _, err := tx.Set(key, val, nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *Tx[mock]) error {
			return tx.Ascend("ages", func(key string, val mock) bool {
				_, err := tx.Delete(key)
				if err != ErrTxIterating {
					t.Fatal("should not be able to call Delete while iterating.")
				}
				_, _, err = tx.Set(key, mock{}, nil)
				if err != ErrTxIterating {
					t.Fatal("should not be able to call Set while iterating.")
				}
				return true
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTx_SetGet(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		_, _, err := tx.Set("keee1", mock{Key: "keee1", Workspace: "wss1", Num: 12}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		val, err := tx.Get("keee1", true)
		if err != nil {
			t.Fatal(err)
		}
		if val.Key != "keee1" {
			t.Fatal("expecting 'keee1', got ", val.Key)
		}
		if val.Workspace != "wss1" {
			t.Fatal("expecting 'wss1', got ", val.Workspace)
		}
		if val.Num != 12 {
			t.Fatal("expecting '12', got ", val.Num)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_SetGetDelete(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		_, _, err := tx.Set("keee1", mock{Key: "keee1", Workspace: "wss1", Num: 12}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		val, err := tx.Get("keee1", true)
		if err != nil {
			t.Fatal(err)
		}
		if val.Key != "keee1" {
			t.Fatal("expecting 'keee1', got ", val.Key)
		}
		if val.Workspace != "wss1" {
			t.Fatal("expecting 'wss1', got ", val.Workspace)
		}
		if val.Num != 12 {
			t.Fatal("expecting '12', got ", val.Num)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx[mock]) error {
		_, err := tx.Delete("keee1")
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		_, err := tx.Get("keee1", true)
		if err != ErrNotFound {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DeleteAll(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		_, _, err := tx.Set("keee1", mock{Key: "keee1", Workspace: "wss1", Num: 12}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx[mock]) error {
		err := tx.DeleteAll()
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	length, _ := db.Len()
	if length != 0 {
		t.Fatal("expecting 0 items, got ", length)
	}
}

func TestDB_String(t *testing.T) {
	str := "test"
	db, err := Open[string](":memory:", str)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	//check length of db
	length, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db length: %d", length)

	if err := db.Update(func(tx *Tx[string]) error {
		key := fmt.Sprintf("key%d", rand.Int())
		t.Logf("key: %s", key)
		_, _, err := tx.Set(key, str, nil)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	//check length of db
	length, err = db.Len()
	if err != nil {
		t.Fatal(err)
	}
	if length != 1 {
		t.Fatalf("expecting 11, got %d", length)
	}
}

func TestDB_ReadConfig(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	var config Config[mock]
	if err := db.ReadConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.SyncPolicy != 1 {
		t.Fatal("expecting SyncPolicy 1, got ", config.SyncPolicy)
	}
}

func TestDB_SetConfig(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	var config Config[mock]
	if err := db.SetConfig(config); err != nil {
		t.Fatal(err)
	}
	// test set invalid config
	config.SyncPolicy = 3
	if err := db.SetConfig(config); err != ErrInvalidSyncPolicy {
		t.Fatal("expecting ErrInvalidSyncPolicy, got ", err)
	}
}

func TestDB_DropIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("workspace", "*", func(a mock, b mock) bool {
		return a.Workspace < b.Workspace
	})
	_, err := db.Set("keee1", mock{Key: "keee1", Workspace: "wss1", Num: 12}, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = db.DropIndex("workspace")
	if err != nil {
		t.Fatalf("drop index should not fail: %s", err)
	}
}

func TestTx_CreateIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	// fill db
	for i := 0; i < 10; i++ {
		_, err := db.Set(fmt.Sprintf("a%d", i), mock{Key: fmt.Sprintf("key%d", i), Workspace: fmt.Sprintf("wss%d", i), Num: i}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Set(fmt.Sprintf("A%d", i+10), mock{Key: fmt.Sprintf("key%d", i), Workspace: fmt.Sprintf("wss%d", i), Num: i}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	// create basic index should not error
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.CreateIndex("idx1", "*", func(a, b mock) bool { return a.Num < b.Num }); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// create index with same name should error
	if err := db.Update(func(tx *Tx[mock]) error {
		err := tx.CreateIndex("idx1", "a*", func(a, b mock) bool { return a.Num > b.Num })
		return err
	}); err == nil || err != ErrIndexExists {
		t.Fatalf("duplicated index name should error")
	}
	// create unnamed index
	if err := db.Update(func(tx *Tx[mock]) error {
		return tx.CreateIndex("", "a*", func(a, b mock) bool { return a.Num > b.Num })
	}); err == nil || err != ErrIndexExists {
		t.Fatalf("empty index name should error")
	}
	// create index with case in-sensitive key matching
	if err := db.Update(func(tx *Tx[mock]) error {
		return tx.CreateIndexOptions("idx2", "a*", &IndexOptions{CaseInsensitiveKeyMatching: true}, func(a, b mock) bool { return a.Num > b.Num })
	}); err != nil {
		t.Fatal(err)
	}
	// check index len
	_ = db.View(func(tx *Tx[mock]) error {
		l, _ := tx.IndexLen("idx2")
		if l != 20 {
			t.Fatal("expecting 20 items, got ", l)
		}
		return nil
	})
}

func TestTx_DropIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.CreateIndex("idx1", "*", func(a, b mock) bool { return a.Num < b.Num }); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// drop index should not error
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.DropIndex("idx1"); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// drop index that does not exist should error
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.DropIndex("idx1"); err == nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_ReplaceIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.CreateIndex("idx1", "*", func(a, b mock) bool { return a.Num < b.Num }); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// replace index should not error
	if err := db.ReplaceIndex("idx1", "a*", func(a, b mock) bool { return a.Num > b.Num }); err != nil {
		t.Fatal(err)
	}
	// replace index that does not exist should error
	if err := db.ReplaceIndex("idx2", "a*", func(a, b mock) bool { return a.Num > b.Num }); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Indexes(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		if err := tx.CreateIndex("idx1", "*", func(a, b mock) bool { return a.Num < b.Num }); err != nil {
			t.Fatal(err)
		}
		if err := tx.CreateIndex("idx2", "*", func(a, b mock) bool { return a.Num > b.Num }); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// get indexes should not error
	idxs, err := db.Indexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(idxs) != 2 {
		t.Fatalf("expecting 2 indexes, got %d", len(idxs))
	}
}

// ASCEND

func TestTx_AscendAll_Struct(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("num", "*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			val := mock{
				Key:       key,
				Workspace: "ws1",
				Num:       i,
			}
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		var i int
		tx.AscendKeys("num", func(key string, val mock) bool {
			if val.Num != i {
				t.Fatalf("AscendKeys: expecting %d, got %d", i, val.Num)
			}
			i++
			return true
		})
		i = 0
		tx.Ascend("num", func(key string, val mock) bool {
			if val.Num != i {
				t.Fatalf("Ascend: expecting %d, got %d", i, val.Num)
			}
			i++
			return true
		})
		i = 0
		tx.AscendGreaterOrEqual("num", PivotKV[mock]{k: "", v: mock{Num: 8}}, func(key string, value mock) bool {
			if value.Num < 8 {
				t.Fatalf("AscendGreaterOrEqual: expecting >= 8, got %d", value.Num)
			}
			return true
		})
		tx.AscendLessThan("int", PivotKV[mock]{v: mock{Num: 4}}, func(key string, value mock) bool {
			if value.Num >= 4 {
				t.Fatalf("AscendLessThan: expecting < 4, got %d", value.Num)
			}
			return true
		})
		tx.AscendRange("int", PivotKV[mock]{v: mock{Num: 4}}, PivotKV[mock]{v: mock{Num: 8}}, func(key string, value mock) bool {
			t.Logf("ASCENDRANGE: %s %v", key, value)
			if value.Num < 4 || value.Num >= 8 {
				t.Fatalf("AscendRange: expecting >= 4 and < 8, got %d", value.Num)
			}
			return true
		})
		tx.AscendEqual("int", PivotKV[mock]{v: mock{Num: 4}}, func(key string, value mock) bool {
			if value.Num != 4 {
				t.Fatalf("AscendEqual: expecting 4, got %d", value.Num)
			}
			return true
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendAll_Int(t *testing.T) {
	db, err := Open[int64]("", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.CreateIndex("int", "*", func(a, b int64) bool {
		//return a < b
		return b > a
	})
	if err := db.Update(func(tx *Tx[int64]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", rand.Int())
			_, _, err := tx.Set(key, int64(i), nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[int64]) error {
		tx.Ascend("int", func(key string, value int64) bool {
			t.Logf("ASCEND: %s %v", key, value)
			return true
		})
		tx.AscendGreaterOrEqual("int", PivotKV[int64]{k: "", v: int64(8)}, func(key string, value int64) bool {
			t.Logf("ASCENDGE: %s %v", key, value)
			return true
		})
		tx.AscendLessThan("int", PivotKV[int64]{v: 3}, func(key string, value int64) bool {
			t.Logf("ASCENDLT: %s %v", key, value)
			return true
		})
		tx.AscendRange("int", PivotKV[int64]{v: 4}, PivotKV[int64]{v: 8}, func(key string, value int64) bool {
			t.Logf("ASCENDRANGE: %s %v", key, value)
			return true
		})
		tx.AscendEqual("int", PivotKV[int64]{v: 4}, func(key string, value int64) bool {
			t.Logf("ASCENDEQUAL: %s %v", key, value)
			return true
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendKeys(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("keee%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss1", Num: 12}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		var keys []string
		err := tx.AscendKeys("keee*", func(key string, val mock) bool {
			keys = append(keys, key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 10 {
			t.Fatal("expecting 10 keys, got ", len(keys))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_Ascend(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("workspace", "*", func(a, b mock) bool {
		return a.Workspace < b.Workspace
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", rand.Int())
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss1" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.Ascend("workspace", func(key string, val mock) bool {
			t.Logf("key: %s, val: %+v", key, val)
			wsExpected := fmt.Sprintf("wss1%d", i)
			if val.Workspace != wsExpected {
				t.Fatalf("expecting '%v', got '%v'", wsExpected, val.Workspace)
			}
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendEqual(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("workspace", "*", func(a, b mock) bool {
		return a.Workspace < b.Workspace
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 18; i++ {
			key := fmt.Sprintf("key%d", rand.Int())
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i%6), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.AscendEqual("workspace", PivotKV[mock]{k: "", v: mock{Workspace: "wss5"}}, func(key string, val mock) bool {
			t.Logf("key: %s, val: %+v", key, val)
			if val.Workspace != "wss5" {
				t.Fatalf("expecting 'wss5', got '%v'", val.Workspace)
			}
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != 3 {
			t.Fatalf("expecting 3, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendGreaterOrEqual(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("workspace", "*", func(a, b mock) bool {
		//compare two int values inside the string
		numOfStringA, _ := strconv.Atoi(a.Workspace[3:])
		numOfStringB, _ := strconv.Atoi(b.Workspace[3:])
		return numOfStringA < numOfStringB
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", rand.Int())
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.AscendGreaterOrEqual("workspace", PivotKV[mock]{k: "", v: mock{Workspace: "wss10"}}, func(key string, val mock) bool {
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != 10 {
			t.Fatalf("expecting 6, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendLessThan(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("num", "*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.AscendLessThan("num", PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != 5 {
			t.Fatalf("expecting 5, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AscendRange(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("num", "*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// check length of db
	length, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db length: %d", length)

	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.AscendRange("num", PivotKV[mock]{v: mock{Num: 5}}, PivotKV[mock]{v: mock{Num: 7}}, func(key string, val mock) bool {
			t.Logf("key: %s, val: %+v", key, val)
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expecting 2, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// DESCEND

func TestTx_DescendAll_Struct(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("num", "*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[mock]) error {
		i := 9
		tx.DescendKeys("key*", func(key string, val mock) bool {
			numFromKey, _ := strconv.Atoi(key[3:])
			if numFromKey != i {
				t.Fatalf("DescendKeys: expecting %d, got %d", i, numFromKey)
			}
			i--
			return true
		})
		if i != -1 {
			t.Fatalf("DescendKeys: expecting 10, got %d", 9-i)
		}
		i = 9
		tx.Descend("num", func(key string, val mock) bool {
			numFromKey, _ := strconv.Atoi(key[3:])
			if numFromKey != i {
				t.Fatalf("Descend: expecting %d, got %d", i, numFromKey)
			}
			i--
			return true
		})
		if i != -1 {
			t.Fatalf("Descend: expecting 10, got %d", 9-i)
		}
		i = 0
		tx.DescendLessOrEqual("num", PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			if val.Num > 5 {
				t.Fatalf("DescendLessOrEqual: expecting <= 5, got %d", val.Num)
			}
			i++
			return true
		})
		if i != 6 {
			t.Fatalf("DescendLessOrEqual: expecting 6 results, got %d", i)
		}
		i = 0
		tx.DescendGreaterThan("num", PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			if val.Num <= 5 {
				t.Fatalf("DescendGreaterThan: expecting > 5, got %d", val.Num)
			}
			i++
			return true
		})
		if i != 4 {
			t.Fatalf("DescendGreaterThan: expecting 4 results, got %d", i)
		}
		i = 0
		tx.DescendRange("num", PivotKV[mock]{v: mock{Num: 7}}, PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			if val.Num <= 5 || val.Num > 7 {
				t.Fatalf("DescendRange: expecting 5 < val <= 7, got %d", val.Num)
			}
			i++
			return true
		})
		if i != 2 {
			t.Fatalf("DescendRange: expecting 2 results, got %d", i)
		}
		i = 0
		tx.DescendEqual("num", PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			if val.Num != 5 {
				t.Fatalf("DescendEqual: expecting 5, got %d", val.Num)
			}
			i++
			return true
		})
		if i != 1 {
			t.Fatalf("DescendEqual: expecting 1 result, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DescendAll_Int(t *testing.T) {
	db, err := Open[int]("", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.CreateIndex("num", "*", func(a, b int) bool {
		return a < b
	})
	if err := db.Update(func(tx *Tx[int]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, i, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[int]) error {
		i := 9
		tx.DescendKeys("key*", func(key string, val int) bool {
			numFromKey, _ := strconv.Atoi(key[3:])
			if numFromKey != i {
				t.Fatalf("DescendKeys: expecting %d, got %d", i, numFromKey)
			}
			i--
			return true
		})
		if i != -1 {
			t.Fatalf("DescendKeys: expecting 10, got %d", 9-i)
		}
		i = 9
		tx.Descend("num", func(key string, val int) bool {
			numFromKey, _ := strconv.Atoi(key[3:])
			if numFromKey != i {
				t.Fatalf("Descend: expecting %d, got %d", i, numFromKey)
			}
			i--
			return true
		})
		if i != -1 {
			t.Fatalf("Descend: expecting 10, got %d", 9-i)
		}
		i = 0
		tx.DescendLessOrEqual("num", PivotKV[int]{v: 5}, func(key string, val int) bool {
			if val > 5 {
				t.Fatalf("DescendLessOrEqual: expecting <= 5, got %d", val)
			}
			i++
			return true
		})
		if i != 6 {
			t.Fatalf("DescendLessOrEqual: expecting 6 results, got %d", i)
		}
		i = 0
		tx.DescendGreaterThan("num", PivotKV[int]{v: 5}, func(key string, val int) bool {
			if val <= 5 {
				t.Fatalf("DescendGreaterThan: expecting > 5, got %d", val)
			}
			i++
			return true
		})
		if i != 4 {
			t.Fatalf("DescendGreaterThan: expecting 4 results, got %d", i)
		}
		i = 0
		tx.DescendRange("num", PivotKV[int]{v: 7}, PivotKV[int]{v: 5}, func(key string, val int) bool {
			if val <= 5 || val > 7 {
				t.Fatalf("DescendRange: expecting 5 < val <= 7, got %d", val)
			}
			i++
			return true
		})
		if i != 2 {
			t.Fatalf("DescendRange: expecting 2 results, got %d", i)
		}
		i = 0
		tx.DescendEqual("num", PivotKV[int]{v: 5}, func(key string, val int) bool {
			if val != 5 {
				t.Fatalf("DescendEqual: expecting 5, got %d", val)
			}
			i++
			return true
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DescendKeys(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("keee%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss1", Num: 12}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		var keys []string
		err := tx.DescendKeys("kee*", func(key string, val mock) bool {
			keys = append(keys, key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 10 {
			t.Fatal("expecting 10 keys, got ", len(keys))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_Descend(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("workspace", "*", func(a, b mock) bool {
		return a.Workspace < b.Workspace
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", rand.Int())
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss1" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// check length of db
	length, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	if length != 10 {
		t.Fatal("expecting 10, got ", length)
	}

	if err := db.View(func(tx *Tx[mock]) error {
		i := 9
		err := tx.Descend("workspace", func(key string, val mock) bool {
			//t.Logf("key: %s, val: %+v", key, val)
			wsExpected := fmt.Sprintf("wss1%d", i)
			if val.Workspace != wsExpected {
				t.Fatalf("expecting '%v', got '%v'", wsExpected, val.Workspace)
			}
			i--
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DescendLessOrEqual(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex("num", "*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx[mock]) error {
		i := 0
		err := tx.DescendLessOrEqual("num", PivotKV[mock]{v: mock{Num: 5}}, func(key string, val mock) bool {
			i++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != 6 {
			t.Fatalf("expecting 6, got %d", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TTL

func TestTx_TTL(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	err := db.Update(func(tx *Tx[mock]) error {
		if _, _, err := tx.Set("key1", mock{
			Key: "key1",
		}, &SetOptions{Expires: true, TTL: time.Second}); err != nil {
			return err
		}
		if _, _, err := tx.Set("key2", mock{
			Key: "key2",
		}, nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx[mock]) error {
		dur1, err := tx.TTL("key1")
		if err != nil {
			t.Fatal(err)
		}
		if dur1 > time.Second || dur1 <= 0 {
			t.Fatalf("expecting between zero and one second, got '%v'", dur1)
		}
		dur1, err = tx.TTL("key2")
		if err != nil {
			t.Fatal(err)
		}
		if dur1 >= 0 {
			t.Fatalf("expecting a negative value, got '%v'", dur1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_TTLAfterReopen(t *testing.T) {
	ttl := time.Second * 3
	db := testOpen(t)
	defer testClose(db)
	_, err := db.Set("key1", mock{
		Key: "key1",
	}, &SetOptions{Expires: true, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	db = testReOpenDelay(t, db, ttl/4)
	err = db.View(func(tx *Tx[mock]) error {
		val, err := tx.Get("key1")
		if err != nil {
			return err
		}
		if val.Key != "key1" {
			t.Fatalf("expecting '%v', got '%v'", "val1", val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db = testReOpenDelay(t, db, ttl-ttl/4)
	defer testClose(db)
	err = db.View(func(tx *Tx[mock]) error {
		val, err := tx.Get("key1")
		if err == nil || err != ErrNotFound {
			t.Fatal("expecting not found")
		}
		if val != nil {
			t.Fatalf("expecting '%v', got '%v'", nil, val)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Len

func TestTx_Len(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// check length of db
	err := db.View(func(tx *Tx[mock]) error {
		n, err := tx.Len()
		if err != nil {
			t.Fatal(err)
		}
		if n != 10 {
			t.Fatalf("expecting 10, got %d", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_IndexLen(t *testing.T) {
	idxName := "idx1"
	db := testOpen(t)
	defer testClose(db)
	db.CreateIndex(idxName, "idx*", func(a, b mock) bool {
		return a.Num < b.Num
	})
	if err := db.Update(func(tx *Tx[mock]) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 12; i++ {
			key := fmt.Sprintf("idx%d", i)
			_, _, err := tx.Set(key, mock{Key: key, Workspace: "wss" + strconv.Itoa(i), Num: i}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// check length of db
	l, _ := db.Len()
	if l != 22 {
		t.Fatalf("expecting 22, got %d", l)
	}
	// check length of num index
	err := db.View(func(tx *Tx[mock]) error {
		n, err := tx.IndexLen(idxName)
		if err != nil {
			t.Fatal(err)
		}
		if n != 12 {
			t.Fatalf("expecting 12, got %d", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// check length of invalid index
	err = db.View(func(tx *Tx[mock]) error {
		_, err := tx.IndexLen("invalid")
		if err == nil {
			t.Fatal("expecting error")
		}
		return nil
	})
	//check length of empty index
	err = db.View(func(tx *Tx[mock]) error {
		le, err := tx.IndexLen("")
		if err != nil {
			t.Fatal(err)
		}
		if le != l {
			return nil
		}
		return nil
	})
}
