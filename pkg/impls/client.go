package impls

type KeyValueExtractorStruct[K comparable, V any] struct {
}
type DelegatorStruct[K comparable, V any] struct {
}
type RetryHandlerStruct[K comparable, V any] struct {
}
type DLQHandlerStruct[K comparable, V any] struct {
}

func (kve KeyValueExtractorStruct[K, V]) Extract(source any) (key K, value V) {
	return key, value
}
func (kve DelegatorStruct[K, V]) Delegate(source any) error {
	return nil
}
func (kve RetryHandlerStruct[K, V]) RetryHandle(source any) error {
	return nil
}
func (kve DLQHandlerStruct[K, V]) DLQHandle(source any) error {
	return nil
}

//
//func (kve Aggregator)
