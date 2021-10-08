package udp

import (
	"sync"

	"go.dedis.ch/cs438/transport"
)

type SafePacketSlice struct {
	slice []transport.Packet
	sync  sync.Mutex
}

// returns a new empty SafePacketSlice
func NewSafePacketSlice() SafePacketSlice {
	return SafePacketSlice{
		slice: make([]transport.Packet, 0),
	}
}

// appends the given element to the underlying slice
func (slice *SafePacketSlice) Append(packet transport.Packet) {
	slice.sync.Lock()
	defer slice.sync.Unlock()

	slice.slice = append(slice.slice, packet)
}

// Returns the underlying "unsafe" slice
// the returned slice should not be modified
//TODO return a copy ?
func (slice *SafePacketSlice) Get() []transport.Packet {
	return slice.slice
}
