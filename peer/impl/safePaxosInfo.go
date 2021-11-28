package impl

import (
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
)

type SafePaxosInfo struct {
	clock         uint
	maxID         uint
	acceptedID    uint
	acceptedValue *types.PaxosValue

	nextID    uint
	nbPeers   uint
	threshold uint

	phase uint

	promises map[uint]map[string]struct{}
	accepted map[string]map[string]struct{}

	lock    sync.Mutex
	runlock sync.Mutex
	myID    uint
	myValue *types.PaxosValue
}

func NewSafePaxosInfo(nextID, nbPeers, threshold uint) SafePaxosInfo {
	return SafePaxosInfo{
		clock:      0,
		maxID:      0,
		acceptedID: 0,
		phase:      0,
		nextID:     nextID,
		threshold:  threshold,
		nbPeers:    nbPeers,
		promises:   map[uint]map[string]struct{}{},
		accepted:   map[string]map[string]struct{}{},
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

func (pi *SafePaxosInfo) HandlePropose(prop *types.PaxosProposeMessage) (types.PaxosAcceptMessage, bool) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	if prop.Step != pi.clock || prop.ID != pi.maxID {
		return types.PaxosAcceptMessage{}, false
	}

	pi.acceptedValue = &prop.Value
	pi.acceptedID = prop.ID

	acc := types.PaxosAcceptMessage{
		Step:  prop.Step,
		ID:    prop.ID,
		Value: prop.Value,
	}
	return acc, true
}

func (pi *SafePaxosInfo) HandleAccept(n *node, source string, acc *types.PaxosAcceptMessage) bool {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	if acc.Step != pi.clock {
		return false
	}

	_, ok := pi.accepted[acc.Value.UniqID]
	if !ok {
		pi.accepted[acc.Value.UniqID] = make(map[string]struct{})
	}

	pi.accepted[acc.Value.UniqID][source] = struct{}{}
	ok = uint(len(pi.accepted[acc.Value.UniqID])) >= pi.threshold

	if ok {
		pi.acceptedValue = &acc.Value
		pi.acceptedID = acc.ID
		pi.phase = 3
	}

	return ok
}

func (pi *SafePaxosInfo) HandlePromise(source string, prom *types.PaxosPromiseMessage) (types.PaxosProposeMessage, bool) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	if pi.phase != 1 || prom.Step != pi.clock {
		return types.PaxosProposeMessage{}, false
	}

	_, ok := pi.promises[prom.ID]
	if !ok {
		pi.promises[prom.ID] = make(map[string]struct{})
	}

	if prom.AcceptedValue != nil && prom.AcceptedID > pi.acceptedID {
		pi.acceptedValue = prom.AcceptedValue
		pi.acceptedID = prom.AcceptedID
	}

	pi.promises[prom.ID][source] = struct{}{}
	ok = uint(len(pi.promises[prom.ID])) >= pi.threshold

	if !ok {
		return types.PaxosProposeMessage{}, false
	}

	prop := types.PaxosProposeMessage{
		Step:  0,
		ID:    pi.myID,
		Value: *pi.myValue,
	}

	if pi.acceptedValue != nil {
		prop.ID = pi.acceptedID
		prop.Value = *pi.acceptedValue
	}

	pi.phase = 2

	return prop, true
}

func broadcastMsg(n *node, msg types.Message) error {
	trmsg, err := n.TypeToTransportMessage(msg)
	if err != nil {
		return err
	}

	return n.Broadcast(trmsg)
}

// broadcasts a new prepare message
func (pi *SafePaxosInfo) broadcastPrepare(n *node, id uint) error {
	prep := types.PaxosPrepareMessage{
		Step:   pi.clock,
		ID:     id,
		Source: n.GetAddress(),
	}

	return broadcastMsg(n, prep)
}

func (pi *SafePaxosInfo) Start(n *node, name, mh string) error {
	pi.lock.Lock()
	pi.myID = pi.nextID
	pi.nextID += pi.nbPeers
	pi.myValue = &types.PaxosValue{
		UniqID:   xid.New().String(),
		Filename: name,
		Metahash: mh,
	}
	pi.phase = 1
	pi.lock.Unlock()

	err := pi.broadcastPrepare(n, pi.myID)
	if err != nil {
		return err
	}

	n.sync.Lock()
	rt := n.rt
	n.sync.Unlock()
	if rt == nil {
		return nil //errors.New("the node is stopped")
	}
	done := rt.context.Done()

	ticker := time.NewTicker(n.conf.PaxosProposerRetry)

	for {
		select {
		case <-ticker.C:
			if pi.phase == 3 {
				return nil
			}

			if pi.phase == 1 {
				pi.lock.Lock()
				pi.myID = pi.nextID
				pi.nextID += pi.nbPeers
				pi.promises[pi.myID] = map[string]struct{}{}
				err = pi.broadcastPrepare(n, pi.myID)
				pi.lock.Unlock()

				if err != nil {
					log.Warn().Err(err).Msg("paxos broadcast prepare")
				}
			}

			if pi.phase == 2 {
				pi.phase = 1 // back to phase 1
			}
		case <-done:
			return nil //errors.New("the node is stopped")
		}
	}
}
