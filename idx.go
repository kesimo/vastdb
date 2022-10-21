package vastdb

import (
	"github.com/kesimo/vastdb/internal/tree"
	"github.com/tidwall/match"
	"strings"
)

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting Key/values.
	CaseInsensitiveKeyMatching bool
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index[T any] struct {
	btr     tree.Btree[*dbItem[T]] // contains the items
	name    string                 // name of the index
	pattern string                 // a required Key pattern
	less    func(a, b T) bool      // less comparison function
	db      *DB[T]                 // the origin database
	opts    IndexOptions           // index options
}

// match the pattern to the Key
func (idx *index[_]) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index[T]) clearCopy() *index[T] {
	// copy the index meta information
	nidx := &index[T]{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = tree.NewGBtree[*dbItem[T]](lessCtx[T](nil))
	}
	return nidx
}

// rebuild the index
func (idx *index[T]) rebuild() {
	// initialize trees
	if idx.less != nil {
		idx.btr = tree.NewGBtree[*dbItem[T]](lessCtx[T](idx))
	}
	// iterate through all keys and fill the index
	idx.db.keys.Ascend(func(item *dbItem[T]) bool {
		dbi := item
		if !idx.match(dbi.key) {
			// continue if pattern doesn't match
			return true
		}
		if idx.less != nil {
			_, err := idx.btr.Set(&dbi)
			if err != nil {
				return false
			}
		}
		return true
	})
}
