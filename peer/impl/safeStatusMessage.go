package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

type SafeStatusMessage struct {
	sync   sync.Mutex
	status types.StatusMessage
}

// Initializes and returns an empty SafeStatusMessage.
func NewSafeStatusMessage() SafeStatusMessage {
	return SafeStatusMessage{status: make(types.StatusMessage)}
}

// returns whether the given value is the next expected message identifier
// for the given address.
// If so, increases the number.
// Returns whether a change was made, and the number of the last packet received.
func (stm *SafeStatusMessage) IsNext(dest string, val uint) (uint, bool) {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	real := stm.status[dest]
	if val == real+1 {
		stm.status[dest] = val
		return val, true
	}
	return real, false
}

func (stm *SafeStatusMessage) Copy() types.StatusMessage {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	cpy := make(types.StatusMessage)
	for key, val := range stm.status {
		cpy[key] = val
	}

	return cpy
}
