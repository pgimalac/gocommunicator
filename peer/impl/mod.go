package impl

import (
	"errors"
	"fmt"
	"sync"
	"time"

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
	}

	// register the callback for each message type
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.HandleChatmessage)
	conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.HandleRumorsMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.HandleAckMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.HandleStatusMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.HandleEmptyMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.HandlePrivateMessage)

	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer

	conf peer.Configuration

	rt *Runtime

	routingTable SafeRoutingTable

	sync sync.Mutex
}

type Runtime struct {
	queueRec, queueSend      *SafePacketQueue
	tpRead, tpHandle, tpSend *AutoThreadPool
}

type Msg struct {
	pkt  transport.Packet
	dest string
}

// Start implements peer.Service
// It returns an error if the service is already started.
func (n *node) Start() error {
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
	tpRead := NewAutoThreadPool(2, 1e6, func(timeout time.Duration) (Msg, error) {
		pkt, err := n.conf.Socket.Recv(timeout)
		return Msg{pkt, ""}, err
	}, queueRec.Push, "read pool", time.Second*5)

	// the handle thread pool
	// handles received packets, and possibly adds packets to send to queueSend
	tpHandle := NewAutoThreadPool(5, 1e6, queueRec.Pop, n.HandleMsg, "handle pool", time.Second*5)

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

	n.rt = &Runtime{
		queueRec, queueSend, tpRead, tpHandle, tpSend,
	}

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
		n.sync.Unlock()
		return errors.New("the service is already stopped")
	}

	n.rt.queueRec.Stop()
	n.rt.queueSend.Stop()
	n.rt.tpRead.Stop()
	n.rt.tpHandle.Stop()
	n.rt.tpSend.Stop()

	n.rt = nil

	return nil
}

func (n *node) GetAddress() string {
	return n.conf.Socket.GetAddress()
}

func (n *node) GetRelay(dest string) (string, bool) {
	return n.routingTable.GetRelay(dest)
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	log.Info().
		Str("by", n.GetAddress()).
		Str("destination", dest).
		Str("type", msg.Type).
		Bytes("payload", msg.Payload).
		Msg("send unicast message")

	relay, exists := n.GetRelay(dest)
	if !exists {
		return fmt.Errorf("there is no relay for the address %s", dest)
	}

	header := transport.NewHeader(n.GetAddress(), n.GetAddress(), dest, 0) // don't care about ttl for now

	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	return n.conf.Socket.Send(relay, pkt, 0) // for now we don't care about the timeout
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, peer := range addr {
		log.Info().Str("address", peer).Msg("add peer")
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
