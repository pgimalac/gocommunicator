package impl

import (
	"errors"
	"sync"

	"go.dedis.ch/cs438/types"
)

type SafeStatusMessage struct {
	sync     sync.Mutex
	messages map[string][]types.Rumor
}

// Initializes and returns an empty SafeStatusMessage.
func NewSafeStatusMessage() SafeStatusMessage {
	return SafeStatusMessage{
		messages: make(map[string][]types.Rumor),
	}
}

// If the given rumor is the next expected for the given peer, stores it.
// Returns whether a change was made, and the number of the last packet received.
func (stm *SafeStatusMessage) ProcessRumor(msg types.Rumor) (uint, bool) {
	peer := msg.Origin
	val := msg.Sequence

	stm.sync.Lock()
	defer stm.sync.Unlock()

	real := uint(len(stm.messages[peer]))
	if val == real+1 {
		stm.messages[peer] = append(stm.messages[peer], msg)
		return val, true
	}
	return real, false
}

// Returns the last rumor received for the given peer, or an error if there is none.
func (stm *SafeStatusMessage) GetLast(peer string) (types.Rumor, error) {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	rumorList := stm.messages[peer]
	if rumorList == nil {
		return types.Rumor{}, errors.New("no rumor received from this peer")
	}
	return rumorList[len(rumorList)-1], nil
}

// Returns the rumor with the given sequence number for the given peer, or an error if there is none.
func (stm *SafeStatusMessage) GetRumor(peer string, sequence uint) (types.Rumor, error) {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	rumorList := stm.messages[peer]
	if uint(len(rumorList)) < sequence {
		return types.Rumor{}, errors.New("sequence number is higher than the number of rumors received from this peer")
	}

	return rumorList[sequence-1], nil
}

// Appends the rumors of the given peer from the given sequence number to the given slice,
// and returns the result.
func (stm *SafeStatusMessage) AppendRumorsTo(peer string, list []types.Rumor, start uint) []types.Rumor {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	return append(list, stm.messages[peer][start-1:]...)
}

// Returns the sequence number of the last received rumor for the given peer.
func (stm *SafeStatusMessage) GetLastNum(peer string) uint {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	return uint(len(stm.messages[peer]))
}

// Creates a type.StatusMessage corresponding to the SafeStatusMessage and returns it.
// The type.StatusMessage is a copy of the state of the SafeStatusMessage at a given time.
func (stm *SafeStatusMessage) Copy() types.StatusMessage {
	stm.sync.Lock()
	defer stm.sync.Unlock()

	cpy := make(types.StatusMessage)
	for peer, list := range stm.messages {
		cpy[peer] = uint(len(list))
	}

	return cpy
}
