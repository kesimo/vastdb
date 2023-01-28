// This file is part of the vastDB project.
// Author: Kevin Eder
// Creation date: 15.10.2022
// License: MIT
// Use of this source code is governed by a MIT license that can be found in the LICENSE file
// at https://github.com/kesimo/vastdb/blob/main/LICENSE

package tree

type Store[T any] interface {
	Set(item *T) (prev *T, err error)
	Delete(key *T) (prev *T, err error)
	Get(key *T) (value *T, ok bool)
	Walk(iter func(items []T))
	Ascend(iter func(item T) bool)
	AscendLT(lt *T, iter func(item T) bool)
	AscendGTE(gte *T, iter func(item T) bool)
	AscendRange(gte, lt *T, iter func(item T) bool)
	Descend(iter func(item T) bool)
	DescendLTE(lte *T, iter func(item T) bool)
	DescendGT(gt *T, iter func(item T) bool)
	DescendRange(lte, gt *T, iter func(item T) bool)
	Len() int
}
