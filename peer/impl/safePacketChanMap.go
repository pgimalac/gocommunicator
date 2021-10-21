package impl

import "sync"

// A safe map from string to channel string.
// Used to map a packet id to a channel,
// on which a routine is waiting for an ack.
type SafePacketChanMap struct {
	sync     sync.Mutex
	channels map[string]chan string
}

// Creates a new empty SafePacketChanMap.
func NewSafePacketChanMap() SafePacketChanMap {
	return SafePacketChanMap{
		channels: make(map[string]chan string),
	}
}

// Adds the given packet and channel to the map.
func (spcm *SafePacketChanMap) AddChannel(packet string, ch chan string) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	spcm.channels[packet] = ch
}

// Removes the given packet id from the map.
func (spcm *SafePacketChanMap) RemoveChannel(packet string) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	delete(spcm.channels, packet)
}

// Returns the channel associated to the packet id.
func (spcm *SafePacketChanMap) GetChannel(packet string) chan string {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	return spcm.channels[packet]
}
