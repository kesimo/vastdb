package vastdb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

type mockB struct {
	Key       string `json:"key"`
	Workspace string `json:"workspace"`
	Num       int    `json:"num"`
	Boolean   bool   `json:"boolean"`
}

func testSetupDb() (*DB[mockB], error) {
	db, err := Open(":memory:", mockB{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func testSetupPersistentDb() (*DB[mockB], error) {
	db, err := Open("test.db", mockB{})
	if err != nil {
		return nil, err
	}
	db.SetConfig(Config[mockB]{
		SyncPolicy: 1,
	})
	return db, nil
}

func testRemovePersistentDb() error {
	return os.Remove("test.db")
}

func testPrepareStringIndex(db *DB[mockB]) error {
	return db.CreateIndex("workspace", "*", func(a, b mockB) bool {
		return a.Workspace < b.Workspace
	})
}

func testPrepareIntIndex(db *DB[mockB]) error {
	return db.CreateIndex("num", "*", func(a, b mockB) bool {
		return a.Num < b.Num
	})
}

func testPrepareCombinedIndex(db *DB[mockB]) error {
	return db.CreateIndex("num_boolean", "*", func(a, b mockB) bool {
		return a.Num < b.Num
	}, func(a, b mockB) bool {
		return a.Boolean == true
	})
}

func Benchmark_Set(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	//err = testPrepareIntIndex(db)
	//err = testPrepareStringIndex(db)
	//err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(i)
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello" + iStr,
				Workspace: "ws2",
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	len, _ := db.Len()
	if len != b.N {
		b.Errorf("expected %d, got %d", b.N, len)
	}
}

func Benchmark_Set_1_index(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	err = testPrepareIntIndex(db)
	//err = testPrepareStringIndex(db)
	//err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(i)
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello" + iStr,
				Workspace: "ws2",
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
func Benchmark_Set_2_index(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	err = testPrepareIntIndex(db)
	err = testPrepareStringIndex(db)
	//err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(i)
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello" + iStr,
				Workspace: "ws2",
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
func Benchmark_Set_3_index(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	err = testPrepareIntIndex(db)
	err = testPrepareStringIndex(db)
	err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(i)
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello" + iStr,
				Workspace: "ws2",
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Set_Random(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(rand.Int())
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello",
				Workspace: "ws2",
				Num:       i,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// check length of db
	len, err := db.Len()
	if err != nil {
		b.Fatal(err)
	}
	if len != b.N {
		b.Errorf("expected len %d, got %d", b.N, len)
	}
}

func BenchmarkPersistTx_Set(b *testing.B) {
	db, err := testSetupPersistentDb()
	defer testRemovePersistentDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	//err = testPrepareIntIndex(db)
	//err = testPrepareStringIndex(db)
	//err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iStr := strconv.Itoa(i)
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello" + iStr,
				Workspace: "ws2" + iStr,
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTx_Get(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}
	for i := 0; i < 100000; i++ {
		if err := db.Update(func(tx *Tx[mockB]) error {
			_, _, err := tx.Set("key"+strconv.Itoa(i), mockB{
				Key:       "hello",
				Workspace: "ws1",
				Num:       50,
				Boolean:   false,
			}, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	if err := db.View(func(tx *Tx[mockB]) error {
		for i := 0; i < b.N; i++ {
			_, err := tx.Get("key99999", true)
			if err != nil {
				b.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTx_Get_Random(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx[mockB]) error {
		for i := 0; i < 100000; i++ {
			iStr := strconv.Itoa(i)
			_, _, err := tx.Set("hello"+iStr, mockB{
				Key:       "hello",
				Workspace: "ws1",
				Num:       50,
				Boolean:   false,
			}, nil)
			if err != nil {
				return err
			}
		}
		return err
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	if err := db.View(func(tx *Tx[mockB]) error {
		for i := 0; i < b.N; i++ {
			_, _ = tx.Get("hello"+strconv.Itoa(rand.Int()%100000), true)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTx_Get_Parallel(b *testing.B) {
	db, err := testSetupDb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	//err = testPrepareIntIndex(db)
	//err = testPrepareStringIndex(db)
	//err = testPrepareCombinedIndex(db)
	if err != nil {
		b.Errorf("failed to create index: %v", err)
	}

	if err := db.Update(func(tx *Tx[mockB]) error {
		_, _, err := tx.Set("hello", mockB{
			Key:       "hello",
			Workspace: "ws1",
			Num:       50,
			Boolean:   false,
		}, nil)
		return err
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := db.View(func(tx *Tx[mockB]) error {
				_, err := tx.Get("hello", true)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
