![image](docs/vastDB.png)

# vastDB

**vastDB** is a generic and [fast](#performance) ACID key-Value Store with custom indexing.
It's basically a modified Version of [buntdb](https://github.com/tidwall/buntdb) 
with support for generic types and faster field indexing.

The goal of this Project is to provide a generic key-value store, that does not only
perform well on reads, but also on writes. It's not meant to be a replacement for
a full blown database, but rather a tool to store data in a fast and easy way.

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
- [Examples](#examples)
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
// create a custom struct to store in the database
type customItem struct {
	ID       string
	Num       int
}

// create a new database instance by passing in the path to the database file,
// or ":memory:" to create a database that exists only in memory
// add an empty (or filled) object of the custom struct to the database
// interfaces are not supported as well as double-pointers
db, err := Open(":memory:", customItem{})
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
	// deletion of the timeed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key string, value T, tx *Tx[T]) error
}

//Apply the configuration to the database
db.SetConfig(config)
```

### Inserting data

---

```go
// [...]
// insert a new item into the database
err := db.Update(func(tx *vastdb.Tx[customItem]) error {
    // create a new item
    item := customItem{
        ID: "123",
        Num: 123,
    }
    // insert the item into the database
    // the key must be a string
    // the value must be the predefined custom struct
    _, _, err := tx.Set("keyA", &item, nil)
    return err
})
```

### Reading data

---

```go
// [...]
// read an item from the database
err := db.View(func(tx *vastdb.Tx[customItem]) error {
    // the key must be a string
    // the return value's type is the predefined custom struct
    val, err := tx.Get("keyA")
    if err != nil {
        return err
    }
    // do something with the item
    fmt.Println(val.ID)
    return nil
})
```

### Deleting data

---

```go
// [...]
// delete an item from the database
err := db.Update(func(tx *vastdb.Tx[customItem]) error {
    // the key must be a string
    // returns the deleted item if existed or nil
    prev, err := tx.Delete("keyA")
    return err
})
```

### Transactions

transactions can be used to group multiple operations into a single atomic operation.
like updating multiple objects or deleting multiple objects.
there can be multiple transactions running at the same time, but only one transaction
can be writing to the database at a time.
to open a read-only transaction, use the `View` method.
to open a read-write transaction, use the `Update` method.

For example update one field in an item:

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
    _, _, err := tx.Set("keyA", &val, nil)
    return err
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

TODO


