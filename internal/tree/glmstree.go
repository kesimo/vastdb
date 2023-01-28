package tree

type GLMSTree[T any] struct {
}

func NewGLMSTree[T any](less func(a, b T) bool) *GLMSTree[T] {
	return &GLMSTree[T]{}
}

func (G *GLMSTree[T]) Set(item *T) (prev *T, err error) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Delete(key *T) (prev *T, err error) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Get(key *T) (value *T, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Walk(iter func(items []T)) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Ascend(iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) AscendLT(lt *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) AscendGTE(gte *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) AscendRange(gte, lt *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Descend(iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) DescendLTE(lte *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) DescendGT(gt *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) DescendRange(lte, gt *T, iter func(item T) bool) {
	//TODO implement me
	panic("implement me")
}

func (G *GLMSTree[T]) Len() int {
	//TODO implement me
	panic("implement me")
}
