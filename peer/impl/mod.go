package impl

import (
	"errors"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	return &node{
		conf:        conf,
		isStarted:   false,
		returnValue: make(chan error),
		stopSignal:  make(chan struct{}),
	}
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
}

// Start implements peer.Service
// It returns an error if the service is already started.
func (n *node) Start() error {
	log.Info().Msg("start the service")

	if n.isStarted {
		err := errors.New("the service is already started")
		log.Error().Err(err).Msg("")
		return err
	}

	n.isStarted = true

	go func(returnValue chan error, stopSignal chan struct{}) {
		for {
			_, err := n.conf.Socket.Recv(time.Second * 1)
			if err != nil && !errors.Is(err, transport.TimeoutErr(0)) {
				// there is an actual error
				returnValue <- err
				return
			}

			select {
			case <-stopSignal:
				// we received a signal to stop listening
				returnValue <- nil
				return
			default:
			}

			//TODO handle packets
		}
	}(n.returnValue, n.stopSignal)

	return nil
}

// Stop implements peer.Service
// It returns an error if the service is already stopped.
// It blocks until the listening routine stops
func (n *node) Stop() error {
	log.Info().Msg("stop the service")

	if !n.isStarted {
		err := errors.New("the service is already stopped")
		log.Error().Err(err).Msg("")
		return err
	}

	n.stopSignal <- struct{}{}
	err := <-n.returnValue

	if err != nil {
		// the listening routine encountered an error,
		// it did not read the stop signal that was just sent
		<-n.stopSignal
	}

	n.isStarted = false

	return err
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	log.Info().
		Str("destination", dest).
		Str("type", msg.Type).
		Bytes("payload", msg.Payload).
		Msg("send unicast message")
	panic("to be implemented in HW0")
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, peer := range addr {
		log.Info().Str("address", peer).Msg("add peer")
	}
	panic("to be implemented in HW0")
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	log.Info().Msg("get routing table")
	panic("to be implemented in HW0")
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	log.Info().
		Str("origin", origin).
		Str("relay address", relayAddr).
		Msg("set routing entry")
	panic("to be implemented in HW0")
}
