package impl

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	n := node{
		conf:         conf,
		routingTable: NewSafeRoutingTable(conf.Socket.GetAddress()),
		status:       NewSafeStatusMessage(),
		expectedAcks: NewSafeAsyncNotifier(),
		catalog:      NewSafeCatalog(),
	}

	// register the callback for each message type
	conf.MessageRegistry.RegisterMessageCallback(
		types.ChatMessage{},
		n.HandleChatmessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.RumorsMessage{},
		n.HandleRumorsMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.AckMessage{},
		n.HandleAckMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.StatusMessage{},
		n.HandleStatusMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.EmptyMessage{},
		n.HandleEmptyMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.PrivateMessage{},
		n.HandlePrivateMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.DataRequestMessage{},
		n.HandleDataRequestMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.DataReplyMessage{},
		n.HandleDataReplyMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.SearchRequestMessage{},
		n.HandleSearchRequestMessage,
	)
	conf.MessageRegistry.RegisterMessageCallback(
		types.SearchReplyMessage{},
		n.HandleSearchReplyMessage,
	)

	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration

	routingTable SafeRoutingTable
	status       SafeStatusMessage
	expectedAcks SafeAsyncNotifier
	catalog      SafeCatalog

	rumorNum uint

	rt   *Runtime
	sync sync.Mutex
}

type Runtime struct {
	queueRec, queueSend      *SafePacketQueue
	tpRead, tpHandle, tpSend *AutoThreadPool

	context context.Context
	cancel  context.CancelFunc
}

type Msg struct {
	pkt  transport.Packet
	dest string
}

// Variables used to synchronize the initialization of the logger
var mutex sync.Mutex
var initialized bool = false

// Initializes the logger, if not already done
func startLog() {
	mutex.Lock()
	defer mutex.Unlock()

	if !initialized {
		zerolog.TimeFieldFormat = time.RFC3339Nano // for increased time precision
		zerolog.SetGlobalLevel(zerolog.WarnLevel)  // log level
		log.Logger = log.Output(
			zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: "15:04:05.000",
			}).
			With().
			Timestamp().
			// Displays the name of the file and the line where the log was
			// called
			// Caller().
			Logger()

		initialized = true
	}
}

// Start implements peer.Service
// It returns an error if the service is already started.
func (n *node) Start() error {
	startLog()

	log.Info().Msg("start the service")

	if n == nil {
		return errors.New("the given node isn't initialized")
	}

	n.sync.Lock()
	defer n.sync.Unlock()

	if n.rt != nil {
		return errors.New("the service is already started")
	}

	// the queue used to manage Packets that have been received
	// and that are yet to be handled
	queueRec := NewSafePacketQueue("received packets queue")

	// the queue used to manage Packets that have been created
	// and that are yet to be sent
	queueSend := NewSafePacketQueue("to send packets queue")

	// the read thread pool
	// reads Packets from the socket and adds them to queueRec
	tpRead := NewAutoThreadPool(
		2,
		1e6,
		func(timeout time.Duration) (Msg, error) {
			pkt, err := n.conf.Socket.Recv(timeout)
			return Msg{pkt, ""}, err
		},
		queueRec.Push,
		"read pool",
		time.Second*5,
	)

	// the handle thread pool
	// handles received packets, and possibly adds packets to send to queueSend
	tpHandle := NewAutoThreadPool(
		5,
		1e6,
		queueRec.Pop,
		n.HandleMsg,
		"handle pool",
		time.Second*5,
	)

	// the send thread pool
	// sends Packets from queueSend
	tpSend := NewAutoThreadPool(2, 1e6, queueSend.Pop, func(msg Msg) error {
		for {
			err := n.conf.Socket.Send(msg.dest, msg.pkt, time.Second*5)
			if !errors.Is(err, transport.TimeoutErr(0)) {
				return err
			}
		}
	}, "send pool", time.Second*5)

	context, cancel := context.WithCancel(context.Background())

	n.rt = &Runtime{
		queueRec, queueSend, tpRead, tpHandle, tpSend, context, cancel,
	}

	done := context.Done()
	go n.antiEntropy(done)
	go n.heartbeat(done)

	return nil
}

// Stop implements peer.Service
// It returns an error if the service is already stopped.
// It blocks until the listening routine stops
func (n *node) Stop() error {
	log.Info().Msg("stop the service")

	if n == nil {
		return errors.New("the given node isn't initialized")
	}

	n.sync.Lock()
	defer n.sync.Unlock()

	if n.rt == nil {
		return errors.New("the service is already stopped")
	}

	n.rt.tpRead.Stop()
	n.rt.tpHandle.Stop()
	n.rt.tpSend.Stop()

	n.rt.queueRec.Stop()
	n.rt.queueSend.Stop()

	n.rt.cancel()

	n.rt = nil

	return nil
}

// The anti-entropy routine.
// If the AntiEntropyInterval is zero, returns immediately.
// Otherwise, loops until the end of the node,
// and sends the node's status to a random neighbor every AntiEntropyInterval
// If there is no neighbor, nothing happens.
func (n *node) antiEntropy(done <-chan struct{}) {
	if n == nil || n.conf.AntiEntropyInterval == 0 {
		return
	}

	tick := time.NewTicker(n.conf.AntiEntropyInterval)
	for {
		select {
		case <-tick.C:
			n.SendStatusMessage()
		case <-done:
			return
		}
	}
}

// The heartbeat routine
func (n *node) heartbeat(done <-chan struct{}) {
	if n == nil || n.conf.HeartbeatInterval == 0 {
		return
	}

	tick := time.NewTicker(n.conf.HeartbeatInterval)
	for {
		select {
		case <-tick.C:
			msg := types.EmptyMessage{}
			trmsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
			if err == nil {
				n.Broadcast(trmsg)
			} else {
				log.Warn().Str("by", n.GetAddress()).Msg("marshalling empty message failed in heartbeat")
			}
		case <-done:
			return
		}
	}
}

func (n *node) GetAddress() string {
	return n.conf.Socket.GetAddress()
}

func (n *node) GetRelay(dest string) (string, bool) {
	return n.routingTable.GetRelay(dest)
}

func (n *node) IsNeighbor(dest string) bool {
	relay, ok := n.GetRelay(dest)
	return ok && relay == dest
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, peer := range addr {
		log.Info().
			Str("by", n.GetAddress()).
			Str("address", peer).
			Msg("add peer")
		n.SetRoutingEntry(peer, peer) // no relay ?
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	log.Debug().Msg("get routing table")

	return n.routingTable.Copy()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	log.Debug().
		Str("origin", origin).
		Str("relay address", relayAddr).
		Msg("set routing entry")

	// we ignore our own address
	if origin != n.GetAddress() {
		n.routingTable.SetRoutingEntry(origin, relayAddr)
	}
}
