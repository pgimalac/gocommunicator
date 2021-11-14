package impl

import "sync"

// A generic set in which you can add elements
// and check if the element was already there before.
type SafeSet struct {
	set  map[interface{}]struct{}
	sync sync.Mutex
}

// Creates a new empty SafeSet.
func NewSafeSet() SafeSet {
	return SafeSet{
		set: make(map[interface{}]struct{}),
	}
}

// Adds the given key to the set,
// and returns whether the element was already there before.
func (rm *SafeSet) Add(key interface{}) bool {
	rm.sync.Lock()
	defer rm.sync.Unlock()

	_, ok := rm.set[key]
	if !ok {
		rm.set[key] = struct{}{}
	}

	return ok
}
