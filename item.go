package vastdb

import (
	json2 "encoding/json"
	"strconv"
	"time"
)

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}

type dbItem[T any] struct {
	key     string      // the binary key and value
	val     T           // generic value
	opts    *dbItemOpts // optional meta information
	keyless bool        // keyless item for scanning
}

// valueToString converts any value to a string.
func valueToString[T any](val T) (string, error) {
	switch v := any(val).(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.Itoa(int(v)), nil
	case int16:
		return strconv.Itoa(int(v)), nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int64:
		return strconv.Itoa(int(v)), nil
	case uint:
		return strconv.Itoa(int(v)), nil
	case uint8:
		return strconv.Itoa(int(v)), nil
	case uint16:
		return strconv.Itoa(int(v)), nil
	case uint32:
		return strconv.Itoa(int(v)), nil
	case uint64:
		return strconv.Itoa(int(v)), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	default:
		b, err := json2.Marshal(val) //TODO use gob
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

// ValueFromString converts a string to a value of the given type.
func ValueFromString[T any](val string, out T) error {
	if val == "" {
		return nil
	}
	switch v := any(out).(type) {
	case *string:
		*v = val
	case *[]byte:
		*v = []byte(val)
	case *int:
		i, _ := strconv.Atoi(val)
		*v = i
	case *int8:
		i, _ := strconv.Atoi(val)
		*v = int8(i)
	case *int16:
		i, _ := strconv.Atoi(val)

		*v = int16(i)
	case *int32:
		i, _ := strconv.Atoi(val)
		*v = int32(i)
	case *int64:
		i, _ := strconv.Atoi(val)
		*v = int64(i)
	case *uint:
		i, _ := strconv.Atoi(val)
		*v = uint(i)
	case *uint8:
		i, _ := strconv.Atoi(val)
		*v = uint8(i)
	case *uint16:
		i, _ := strconv.Atoi(val)
		*v = uint16(i)
	case *uint32:
		i, _ := strconv.Atoi(val)
		*v = uint32(i)
	case *uint64:
		i, _ := strconv.Atoi(val)
		*v = uint64(i)
	case *float32:
		f, _ := strconv.ParseFloat(val, 32)
		*v = float32(f)
	case *float64:
		f, _ := strconv.ParseFloat(val, 64)
		*v = f
	case *bool:
		if val == "1" {
			*v = true
		} else {
			*v = false
		}
	default:
		return json2.Unmarshal([]byte(val), v) //TODO use gob
	}
	return nil
}

// estIntSize returns the string representation size.
// Has the same result as len(strconv.Itoa(x)).
func estIntSize(x int) int {
	n := 1
	if x < 0 {
		n++
		x *= -1
	}
	for x >= 10 {
		n++
		x /= 10
	}
	return n
}

// estArraySize returns the Array representation size.
func estArraySize(count int) int {
	return 1 + estIntSize(count) + 2
}

// estBulkStringSize returns the string representation size.
func estBulkStringSize(s string) int {
	return 1 + estIntSize(len(s)) + 2 + len(s) + 2
}

// estBulkStructSize returns the estimated size of a struct of generic type T.
func estBulkStructSize[T any](s T) int {
	str, _ := valueToString[T](s)
	return 1 + estIntSize(len(str)) + 2 + len(str) + 2
}

// estAOFSetSize returns an estimated number of bytes that this item will use
// when stored in the aof file.
func (dbi *dbItem[T]) estAOFSetSize() int {
	var n int
	if dbi.opts != nil && dbi.opts.ex {
		n += estArraySize(5)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		//if type is string use string size
		n += estBulkStructSize(dbi.val)
		n += estBulkStringSize("ex")
		n += estBulkStringSize("99") // estimate two byte bulk string
	} else {
		n += estArraySize(3)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStructSize(dbi.val)
	}
	return n
}

// appendArray appends an array to the given buffer.
func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')
	return buf
}

// appendBulkString appends a bulk string to the buffer.
func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// appendBulkStruct appends a struct after converting to string to the buffer.
func appendBulkStruct[T any](buf []byte, s T) []byte {
	str, _ := valueToString[T](s)
	return appendBulkString(buf, str)
}

// writeSetTo writes an item as a single SET record to the bufio Writer.
func (dbi *dbItem[T]) writeSetTo(buf []byte, now time.Time) []byte {
	if dbi.opts != nil && dbi.opts.ex {
		ex := dbi.opts.exat.Sub(now) / time.Second
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkStruct(buf, dbi.val)
		buf = appendBulkString(buf, "ex")
		buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkStruct(buf, dbi.val)
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the bufio Writer.
func (dbi *dbItem[_]) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *dbItem[_]) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *dbItem[_]) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem[T]) Less(dbi2 *dbItem[T], ctx interface{}) bool {
	switch ctx := ctx.(type) {
	case *exctx[T]:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index[T]:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the Key comparison. This creates absolute uniqueness.
	if dbi.keyless {
		return false
	} else if dbi2.keyless {
		return true
	}
	return dbi.key < dbi2.key
}
