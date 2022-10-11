package vastdb

import (
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

func BenchmarkTx_Set(b *testing.B) {
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
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *Tx[mockB]) error {
			_, err := tx.Get("hello", true)
			return err
		}); err != nil {
			b.Fatal(err)
		}
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
