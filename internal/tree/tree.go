package tree

type Btree[T any] interface {
	Set(item *T, hint any) (prev *T, err error)
	Delete(key *T) (prev *T, err error)
	Get(key *T, hint any) (value *T, ok bool)
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
