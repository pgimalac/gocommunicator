package impl

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
)

type SafePaxosInfo struct {
	clock         uint
	maxID         uint
	acceptedID    uint
	acceptedValue *types.PaxosValue

	initID    uint
	nextID    uint
	nbPeers   uint
	threshold uint

	phase uint

	promises map[uint]map[string]struct{}
	accepted map[string]map[string]struct{}
	tlcs     map[uint]uint
	lasttlc  map[uint]struct{}
	tlcblock map[uint]types.BlockchainBlock

	chanAcc chan types.PaxosAcceptMessage
	chanTLC chan types.BlockchainBlock

	lock      sync.Mutex
	runlock   sync.Mutex
	currentID uint
	myValue   *types.PaxosValue
}

func NewSafePaxosInfo(nextID, nbPeers, threshold uint) SafePaxosInfo {
	return SafePaxosInfo{
		clock:      0,
		maxID:      0,
		acceptedID: 0,
		phase:      0,
		nextID:     nextID,
		initID:     nextID,
		threshold:  threshold,
		nbPeers:    nbPeers,
		promises:   map[uint]map[string]struct{}{},
		accepted:   map[string]map[string]struct{}{},
		tlcblock:   map[uint]types.BlockchainBlock{},
		tlcs:       map[uint]uint{},
		lasttlc:    map[uint]struct{}{},
		chanAcc:    make(chan types.PaxosAcceptMessage),
		chanTLC:    make(chan types.BlockchainBlock),
	}
}

// handles prepare messages
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

// handle propose messages
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

// handles accept messages
func (pi *SafePaxosInfo) HandleAccept(n *node, source string, acc *types.PaxosAcceptMessage) error {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	_, ok := pi.lasttlc[pi.clock]
	if acc.Step != pi.clock || ok {
		return nil
	}

	_, ok = pi.accepted[acc.Value.UniqID]
	if !ok {
		pi.accepted[acc.Value.UniqID] = make(map[string]struct{})
	}

	pi.accepted[acc.Value.UniqID][source] = struct{}{}

	ok = uint(len(pi.accepted[acc.Value.UniqID])) >= pi.threshold
	if !ok {
		return nil
	}
	pi.acceptedValue = &acc.Value
	pi.acceptedID = acc.ID

	pi.phase = 3
	println("go to phase", pi.phase, n.GetAddress())
	block := n.computeBlock(*acc)
	tlc := types.TLCMessage{
		Step:  pi.clock,
		Block: block,
	}
	pi.lasttlc[pi.clock] = struct{}{}
	go broadcastMsg(n, tlc)
	return nil
}

// handles Promise message
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
		Step:  pi.clock,
		ID:    pi.currentID,
		Value: *pi.myValue,
	}

	if pi.acceptedValue != nil {
		prop.ID = pi.acceptedID
		prop.Value = *pi.acceptedValue
	}

	pi.phase = 2
	println("go to phase", pi.phase)

	return prop, true
}

// handles TLC messages
func (pi *SafePaxosInfo) HandleTLC(n *node, source string, tlc *types.TLCMessage) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	_, ok := pi.tlcs[tlc.Step]
	if !ok {
		pi.tlcs[tlc.Step] = 0
	}
	pi.tlcs[tlc.Step]++

	pi.tlcblock[tlc.Step] = tlc.Block

	for pi.tlcs[pi.clock] >= pi.threshold {
		block := pi.tlcblock[pi.clock]
		_, ok := pi.lasttlc[pi.clock]
		if !ok {
			// if we haven't already sent a TLC and we're not catching up
			// send a TLC
			mytlc := types.TLCMessage{
				Step:  pi.clock,
				Block: block,
			}
			pi.lasttlc[pi.clock] = struct{}{}
			go broadcastMsg(n, mytlc)
		}

		blockbytes, err := block.Marshal()
		if err != nil {
			log.Warn().Err(err).Msg("handle tlc: marshalling message")
			return
		}

		n.conf.Storage.GetBlockchainStore().Set(hex.EncodeToString(block.Hash), blockbytes)
		n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, block.Hash)
		n.conf.Storage.GetNamingStore().Set(block.Value.Filename, []byte(block.Value.Metahash))
		println(n.GetAddress(), "add block to blockchain", block.Value.Filename, "step", block.Index)

		pi.tick()

		select {
		case pi.chanTLC <- block:
		default:
		}
	}
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

// executed at the end of Start
// resets the fields of the proposer
func (pi *SafePaxosInfo) Stop() {
	pi.lock.Lock()

	pi.currentID = 0
	pi.myValue = nil

	pi.lock.Unlock()
}

// increase the clock and reset the fields that depend on the clock
// must be called with the lock Locked
func (pi *SafePaxosInfo) tick() {
	pi.clock++

	pi.nextID = pi.initID
	pi.phase = 0
	println("go to phase", pi.phase, "step=", pi.clock)

	pi.promises = map[uint]map[string]struct{}{}
	pi.accepted = map[string]map[string]struct{}{}

	pi.maxID = 0
	pi.acceptedID = 0
	pi.acceptedValue = nil
}

func (pi *SafePaxosInfo) Start(n *node, name, mh string) error {
	defer pi.Stop()

	n.sync.Lock()
	rt := n.rt
	n.sync.Unlock()
	if rt == nil {
		return errors.New("the node is stopped")
	}
	done := rt.context.Done()

	pi.lock.Lock()
	pi.currentID = pi.nextID
	pi.nextID += pi.nbPeers
	pi.myValue = &types.PaxosValue{
		UniqID:   xid.New().String(),
		Filename: name,
		Metahash: mh,
	}

	if pi.phase != 3 {
		pi.phase = 1
		println("go to phase", pi.phase, n.GetAddress())
		go pi.broadcastPrepare(n, pi.currentID)
	}
	pi.lock.Unlock()

	ticker := time.NewTicker(n.conf.PaxosProposerRetry)
	defer ticker.Stop()

	for {
		select {
		case <-pi.chanTLC:
			return nil
		case <-ticker.C:
			pi.lock.Lock()
			if pi.phase == 2 {
				pi.phase = 1
				println("go to phase", pi.phase, n.GetAddress())
			}

			if pi.phase == 1 {
				pi.currentID = pi.nextID
				pi.nextID += pi.nbPeers
				pi.promises[pi.currentID] = map[string]struct{}{}
				go pi.broadcastPrepare(n, pi.currentID)
			}
			pi.lock.Unlock()
		case <-done:
			return errors.New("the node is stopped")
		}
	}
}
