package impl

import "sync"

// A safe map from string to channel string.
// Used to map a key id to a channel,
// on which a routine is waiting for an ack.
type SafeAsyncNotifier struct {
	sync     sync.Mutex
	channels map[string]chan string
	//NOTE:
	// possible change: use chan []byte
	// more versatile, allows to return an address or a chunk
	// not really necessary for now but maybe useful later
}

// Creates a new empty SafeAsyncNotifier.
func NewSafeAsyncNotifier() SafeAsyncNotifier {
	return SafeAsyncNotifier{
		channels: make(map[string]chan string),
	}
}

// Adds the given key and channel to the map.
func (spcm *SafeAsyncNotifier) AddChannel(key string, ch chan string) {
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
func (spcm *SafeAsyncNotifier) Notify(key, value string) bool {
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
