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
		isStarted:    false,
		returnValue:  make(chan error),
		stopSignal:   make(chan struct{}),
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

	isStarted bool

	// a channel used by the listening routine to return an error
	returnValue chan error

	// a channel used to send a stop signal to the listening routine
	stopSignal chan struct{}

	routingTable SafeRoutingTable

	sync sync.Mutex
}

// the listening routine
// waits for incoming packets and handles them
// if a message is received on the stopSignal channel, the execution stops after at most the given timeout
// writes an error
func (n *node) listen(timeout time.Duration) {
	for {
		pkt, err := n.conf.Socket.Recv(timeout)
		if err != nil && !errors.Is(err, transport.TimeoutErr(0)) {
			// there is an actual error
			log.Error().Err(err).Msg("")
			n.returnValue <- err
			return
		}

		select {
		case <-n.stopSignal:
			// we received a signal to stop listening
			log.Info().Msg("message received to stop listening")
			n.returnValue <- nil
			return
		default:
		}

		if errors.Is(err, transport.TimeoutErr(0)) {
			continue
		}

		err = n.HandlePacket(pkt)
		if err != nil {
			log.Error().Err(err).Msg("")
			n.returnValue <- err
			return
		}
	}
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

	if n.isStarted {
		return errors.New("the service is already started")
	}

	n.isStarted = true

	go n.listen(time.Second * 1)

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

	if !n.isStarted {
		n.sync.Unlock()
		return errors.New("the service is already stopped")
	}

	n.isStarted = false
	n.stopSignal <- struct{}{}

	n.sync.Unlock()

	err := <-n.returnValue

	if err != nil {
		// the listening routine encountered an error,
		// it did not read the stop signal that was just sent
		<-n.stopSignal
	}

	return err
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
