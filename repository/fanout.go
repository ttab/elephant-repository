package repository

import (
	"context"
	"sync"
)

type FanOut[T any] struct {
	m         sync.RWMutex
	listeners map[chan T]func(v T) bool
}

func NewFanOut[T any]() *FanOut[T] {
	return &FanOut[T]{
		listeners: make(map[chan T]func(v T) bool),
	}
}

// Listen starts a subscription and blocks until the context is cancelled
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
