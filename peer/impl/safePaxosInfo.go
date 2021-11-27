package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

type SafePaxosInfo struct {
	clock uint
	maxID uint

	lock          sync.Mutex
	acceptedID    uint
	acceptedValue *types.PaxosValue
}

func NewSafePaxosInfo() SafePaxosInfo {
	return SafePaxosInfo{
		clock: 0,
		maxID: 0,
	}
}

func (pi *SafePaxosInfo) HandlePrepare(prep *types.PaxosPrepareMessage) (types.PaxosPromiseMessage, bool) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	if prep.Step != pi.clock || prep.ID <= pi.maxID {
		return types.PaxosPromiseMessage{}, false
	}

	pi.maxID = prep.ID

	promise := types.PaxosPromiseMessage{
		Step: prep.Step,
		ID:   prep.ID,

		AcceptedID:    0,
		AcceptedValue: nil,
	}

	if pi.acceptedValue != nil {
		promise.AcceptedID = pi.acceptedID
		promise.AcceptedValue = pi.acceptedValue
	}

	return promise, true
}

func (pi *SafePaxosInfo) HandlePropose(prop *types.PaxosProposeMessage) bool {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	if prop.Step != pi.clock || prop.ID != pi.maxID {
		return false
	}

	pi.acceptedValue = &prop.Value
	pi.acceptedID = prop.ID

	return true
}
