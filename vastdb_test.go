package vastdb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

type mock struct {
	Key       string `json:"Key"`
	Workspace string `json:"Workspace"`
	Num       int    `json:"Num"`
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

func TestDB_BackgroundOperations(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	for i := 0; i < 1000; i++ {
		if err := db.Update(func(tx *Tx[mock]) error {
			for j := 0; j < 200; j++ {
				if _, _, err := tx.Set(fmt.Sprintf("hello%d", j), mock{
					Key:       "hello" + strconv.Itoa(j),
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
	if err := db.Save(f); err != nil {
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