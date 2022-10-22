<p align="center">
  <img height="100" src="docs/vastdb-logo.svg">
</p>
<h1 align="center">vastDB</h1>
<p align="center">Embedded high-performance Key-Value store with custom indexing written in Go.</p>




## Description

**vastDB** is a generic and [performance](#performance) focused ACID key-Value Store with custom indexing.
It's basically a modified Version of [buntdb](https://github.com/tidwall/buntdb) 
with support for generic types and faster field indexing.

The goal of this Project is to provide a generic key-value store, that does not only
perform well on reads, but also on writes and especially for writes with indexing 
on struct properties. This can be achieved by using the struct directly in the
indexing process. This is not possible with buntdb, because it only supports
indexing on string values or on json fields (reduces performance significant).
It's not meant to be a replacement for
a full-blown database, but rather a tool to store and lookup data in a fast and easy way.

## Features

---

- In-memory database for fast reads and writes
- Embeddable with a simple API
- Index fields of the struct for fast queries
- Type safety by generics
- ACID transactions, supporting rollbacks
- Support for multi value indexes
- Iterating data ascending, descending or in range by keys or indexes
- TTL support
- Persisting with Append-Only-File (AOF) and Snapshotting (Always, every x seconds, never)
- Thread safe

## table of contents

---

- [Installation](#installation)
- [Usage](#usage)
    - [Open a database](#open-a-database)
    - [Configuring the database](#configuring-the-database)
    - [Inserting data](#inserting-data)
    - [Reading data](#reading-data)
    - [Deleting data](#deleting-data)
    - [Transactions](#transactions)
    - [Indexes](#indexes)
    - [Iterating data](#iterating-data)
- [Performance](#performance)
- [License](#license)

## Installation

---

add to your project with

```bash
go get github.com/kesimo/vastdb
```

import with

```go
import "github.com/kesimo/vastdb"
```

## Usage

---

### Open a database

---

```go
package main

import "github.com/kesimo/vastdb"

// create a custom struct to store in the database
type customItem struct {
	ID       string
	Num       int
}

// create a new database instance by passing in the path to the database file,
// or ":memory:" to create a database that exists only in memory
// add an empty (or filled) object of the custom struct to the database
// interfaces are not supported as well as double-pointers
db, err := Open(":memory:", customItem{}) //OR Open[customItem]("path/to/db")
if err != nil {
    // handle error
}
defer db.Close()
```

### Configuring the database

---

```go
// [...]
// create configuration and use same data struct as before for generic type
config := &vastdb.Config[customItem]{
        // set the sync type: Never,EverySecond,Always  (default is Always)
        SyncPolicy SyncPolicy 
	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is a function to custom handle actions on expired keys.
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is
	// performing the deletion of expired items. If OnExpired is present then
	// this callback will not be called. If this callback is present, then the
	// deletion of the timed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key string, value T, tx *Tx[T]) error
}

//Apply the configuration to the database
db.SetConfig(config)
```

### Inserting data

---
Insert Data into the database by passing in a key and a value of the custom struct.
If the key already exists, the value will be overwritten and the old value will be returned.

**WARNING: If you use persistent storage, only the exported fields will be stored.**

```go
// [...]
// insert a new item into the database
prev, err := db.Set("keyA", mockExt{
    ID:        "id1",
    Num:        1,
})
// insert a new item into the database with a TTL
prev, err := db.SetWithTTL("keyA", mockExt{
    ID:        "id1",
    Num:        1,
}, &vastdb.SetOptions{TTL: time.Second, Expires: true})
```

### Reading data

---
Read data from the database by passing in a key. 
The value will be returned as a pointer to the custom struct.
If the key does not exist, the returned pointer will be nil.
if the value is expired nothing will be returned.


**WARNING**: 
Do not modify the returned value, as it will be modified in the database as well.
If you want to modify the value, you have to copy it first.
In case you want to modify the value in database, you should use a transaction.

```go
package main

import "github.com/kesimo/vastdb"
// [...]
// read an item from the database
item, err := db.Get("keyA")
if err != nil {
    // handle error
}
// do something with the item
if item.ID != "id1" {
    //...
}
```

### Deleting data

---
Delete data from the database by passing in a key.
The deleted value will be returned as a pointer to the custom struct.

```go
// [...]
// delete an item from the database
prev, err = db.Del("key1")
if err != nil {
t.Errorf("error deleting item: %v", err)
}
```

### Transactions

transactions can be used to group multiple operations into a single atomic operation.
like updating multiple objects or deleting multiple objects.
there can be multiple transactions running at the same time, but only one transaction
can be writing to the database at a time.

to open a read-only transaction, use the `db.View` method.

to open a read-write transaction, use the `db.Update` method.

For example - update one field in an item:

```go
// [...]
// update an item in the database
err := db.Update(func(tx *vastdb.Tx[customItem]) error {
    val, err := tx.Get("keyA")
    if err != nil {
        return err
    }
    // update the item
    val.Num = 321
    // insert the updated item into the database
    _, _, err = tx.Set("keyA", *val, nil)
	if err != nil {
		return err
	}
    return nil
})
```

if an error is returned from the transaction, then the transaction is rolled back.

### Indexes

They can be used to query the database for specific values and speed up get actions
by using the index instead of the key.
For every index a Btree will be created, which will be used to store 
the values in specific order defined by the comparator function.
They can be created on any field of the struct and applied to specific keys by pattern.
Indexes are created by passing a function to the `CreateIndex` method.

```go
// [...]
// create an index on the Num field of our customItem struct
// the first argument is the name of the index that will be used to interact with
// the index will be applied to all keys that start with "key:"
err := db.CreateIndex("num", "key:*",func(a, b *customItem) bool {
    return a.Num < b.Num
})
```

### Iterating data

For iterating over the database, you can use the `Ascend` and `Descend` methods.

for Pivots, you have tu use an object of type `*vastdb.PivotKV[T]`
its a struct that contains the `key` and the `value` of the item
and used for fallback in case the index is empty -> using the `k` property

`*vastdb.PivotKV[customItem]{k: "keyA", v: customItem{Num: 1}}`


Example usage of `AscendLessThan`:

```go
// [...]
// iterate over all items in the database that have a Num field less than 10
err := db.View(func(tx *Tx[mock]) error {
    err := tx.AscendLessThan("num", *vastdb.PivotKV[customItem]{v: customItem{Num: 10}}, 
	func(key string, val mock) bool {
		//should always be true
		if val.Num < 10 { 
			// ...
		}
		// break the iteration if false is returned
		return true
	})
    if err != nil {
    // handle error
    }
    return nil
})
```

Avaliable methods are:

| Method                                                 | Usage                                                                                     |
|--------------------------------------------------------|-------------------------------------------------------------------------------------------|
| AscendKeys(pattern, iterator)                          | Iterate through all Elements that matches the key pattern (e.g. "user:*")                 |
| Ascend(index, iterator)                                | Iterate through all Elements in specified index                                           |
| AscendGreaterOrEqual(index, gte pivotKV, iterator)     | Iterate through all Elements that are greater or equal the specified pivot in index       |
| AscendLessThan(index, lt PivotKV, iterator)            | Iterate through all Elements that are less than the specified pivot in index              |
| AscendRange(index, gte PivotKV, lt PivotKV, iterator)  | Iterates through Elements in specified Range in index                                     |
| AscendEqual(index, eq PivotKV, iterator)               | Iterates through Elements with equal value to specified one                               |
| DescendKeys(pattern, iterator)                         | Equal to "AscendKeys", but in descend order                                               |
| Descend(index, iterator)                               | Equal to "Ascend", but in descend order                                                   |
| DescendLessOrEqual(index, lte PivotKV, iterator)       | Iterates through all Elements that are less or equal the specified pivot in descend order |
| DescendGreaterThan(index, gt PivotKV, iterator)        | Iterates through all Elements that are greater than the specified pivot in descend order  |
| DescendRange(index, lte PivotKV, gt PivotKV, iterator) | Equal to "AscendRange", but in descend order                                              |
| DescendEqual(index, eq PivotKV, iterator)              | Equal to "AscendEqual", but in descend order                                              |


### Performance

---

The performance of the database is heavily dependent on the performance of the comparator function
and the usage of indexes.

for benchmarking we used the following struct:

```go
type mockB struct {
    Key       string
    Workspace string
    Num       int
    Boolean   bool
}
```

Benchmark results of set and get operations:

```
cpu: Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz

Benchmark_Set-8                  1105269          1048 ns/op
Benchmark_Set_1_index-8           721592          1789 ns/op
Benchmark_Set_2_index-8           502111          2583 ns/op
Benchmark_Set_3_index-8           333624          3848 ns/op
Benchmark_Set_Persist-8           120302          10947 ns/op
BenchmarkTx_Get-8                3921300          301.0 ns/op
BenchmarkTx_Get_Random-8         1347905          876.1 ns/op
BenchmarkTx_Get_Parallel-8      13910053          85.15 ns/op
```

### License

---

MIT License

for more information see the LICENSE file

