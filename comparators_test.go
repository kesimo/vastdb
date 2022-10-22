// This file is part of the vastDB project.
// Last modified : Kevin Eder
// Creation date: 17.10.2022
// License: MIT
// Use of this source code is governed by a MIT license that can be found in the LICENSE file
// at https://github.com/kesimo/vastdb/blob/main/LICENSE

package vastdb

import "testing"

type mockCmp struct {
	id    int
	valid bool
}

func TestCombineComparators(t *testing.T) {
	fn1 := func(a, b mockCmp) bool {
		return a.id < b.id
	}

	fn2 := func(a, b mockCmp) bool {
		return a.valid && !b.valid
	}
	//combine fn1 and fn2
	fnComb := combineComparators([]func(a, b mockCmp) bool{fn1, fn2})
	//test fnComb
	if !fnComb(mockCmp{1, true}, mockCmp{2, false}) {
		t.Error("fn1 and fn2 should return true")
	}
	if fnComb(mockCmp{2, false}, mockCmp{1, true}) {
		t.Error("fn1 and fn2 should return false")
	}
	if fnComb(mockCmp{1, true}, mockCmp{1, true}) {
		t.Error("fn1 and fn2 should return false")
	}
	//one comparator
	fnOne := combineComparators([]func(a, b mockCmp) bool{fn1})
	if fnOne(mockCmp{1, true}, mockCmp{2, false}) != fn1(mockCmp{1, true}, mockCmp{2, false}) {
		t.Error("fnOne should return same result as fn1")
	}
	//no comparator
	fnNone := combineComparators([]func(a, b mockCmp) bool{})
	if fnNone(mockCmp{1, true}, mockCmp{2, false}) {
		t.Error("fnNone should return false")
	}

}
