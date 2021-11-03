package impl

import "sync"

// A safe map from string to a generic channel.
// Used to map a key id to a channel,
// on which a routine is waiting for something
// (an ack, a reply, etc)
type SafeAsyncNotifier struct {
	sync     sync.Mutex
	channels map[string]chan interface{}
}

// Creates a new empty SafeAsyncNotifier.
func NewSafeAsyncNotifier() SafeAsyncNotifier {
	return SafeAsyncNotifier{
		channels: make(map[string]chan interface{}),
	}
}

// Adds the given key and channel to the map.
func (spcm *SafeAsyncNotifier) AddChannel(key string, ch chan interface{}) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	spcm.channels[key] = ch
}

// Removes the given keys from the map.
func (spcm *SafeAsyncNotifier) RemoveChannel(keys ...string) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	for _, key := range keys {
		delete(spcm.channels, key)
	}
}

// Writes to the channel associated with the given key.
// Returns whether the value was successfully written.
// Returns false if there is no such key or if the underlying channel is full.
func (spcm *SafeAsyncNotifier) Notify(key string, value interface{}) bool {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	ch, ok := spcm.channels[key]
	if !ok {
		return false
	}

	select {
	case ch <- value:
		return true
	default:
		return false
	}
}
