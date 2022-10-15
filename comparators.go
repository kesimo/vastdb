package vastdb

// combineComparators combines multiple comparators into one.
func combineComparators[T any](comparators []func(a, b T) bool) func(a, b T) bool {
	var less func(a, b T) bool
	switch len(comparators) {
	default:
		// create a compound comparator function.
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
		// no comparator function
	case 1:
		// only one comparator function
		less = comparators[0]
	}
	return less
}
