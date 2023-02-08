package docformat

import (
	"context"
	"sync"
)

type FanOut[T any] struct {
	in chan T

	m         sync.RWMutex
	listeners map[chan T]func(v T) bool
}

func NewFanOut[T any]() *FanOut[T] {
	return &FanOut[T]{
		in:        make(chan T),
		listeners: make(map[chan T]func(v T) bool),
	}
}

func (f *FanOut[T]) Listen(ctx context.Context, l chan T, test func(v T) bool) {
	f.m.Lock()
	f.listeners[l] = test
	f.m.Unlock()

	<-ctx.Done()

	f.m.Lock()
	delete(f.listeners, l)
	f.m.Unlock()
}

func (f *FanOut[T]) Notify(msg T) {
	f.m.RLock()
	defer f.m.RUnlock()

	for msg := range f.in {
		for listener, test := range f.listeners {
			if !test(msg) {
				continue
			}

			select {
			case listener <- msg:
			default:
			}
		}
	}
}
