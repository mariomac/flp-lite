package flow

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

const (
	statusFree = uintptr(iota)
	statusLocked
)

type Record struct {
	status uintptr
	Values
}

type Values map[string]interface{}

type Unlocker func()

func (r *Record) Lock() Unlocker {
	intents := 0
	for !atomic.CompareAndSwapUintptr(&r.status, statusFree, statusLocked) {
		intents++
		fmt.Printf("%p => %d\n", r, intents)
		runtime.Gosched()
	}

	return func() {
		atomic.StoreUintptr(&r.status, statusFree)
	}
}