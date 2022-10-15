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

func TestDB_String(t *testing.T) {
	str := "test"
	db, err := Open[string](":memory:", str)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	//check length of db
	len, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db len: %d", len)

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
	len, err = db.Len()
	if err != nil {
		t.Fatal(err)
	}
	if len != 1 {
		t.Fatalf("expecting 11, got %d", len)
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
	len, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db len: %d", len)

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
	// check len of db
	len, err := db.Len()
	if err != nil {
		t.Fatal(err)
	}
	if len != 10 {
		t.Fatal("expecting 10, got ", len)
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
