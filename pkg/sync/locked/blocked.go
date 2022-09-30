package locked

import "sync"

type Var[T any] struct {
	m *sync.RWMutex
	val T
}

type Unlocker func()

func For[T any](val T) Var[T] {
	return Var[T]{
		m: &sync.RWMutex{},
		val: val,
	}
}

func (v *Var[T]) Read() (T, Unlocker) {
	v.m.RLock()
	return v.val, v.m.RUnlock
}

func (v *Var[T]) Write() (T, Unlocker) {
	v.m.Lock()
	return v.val, v.m.Unlock
}
