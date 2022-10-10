package vastdb

import (
	"strconv"
)

type Stringer interface {
	String() string
}

// combineComparators combines multiple comparators into one.
func combineComparators[T any](comparators []func(a, b T) bool) func(a, b T) bool {
	var less func(a, b T) bool
	switch len(comparators) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b T) bool {
			for i := 0; i < len(comparators)-1; i++ {
				if comparators[i](a, b) {
					return true
				}
				if comparators[i](b, a) {
					return false
				}
			}
			return comparators[len(comparators)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = comparators[0]
	}
	return less
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field. TODO
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return true
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on TODO
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return true
	}
}

// Desc is a helper function that returns a comparator that reverses the
// comparison of the provided comparator.
func Desc[T any](less func(a, b T) bool) func(a, b T) bool {
	return func(a, b T) bool { return less(b, a) }
}
