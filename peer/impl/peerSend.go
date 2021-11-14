package impl

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

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

	header := transport.NewHeader(
		n.GetAddress(),
		n.GetAddress(),
		dest,
		0,
	) // don't care about ttl for now

	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	return n.conf.Socket.Send(
		relay,
		pkt,
		0,
	) // for now we don't care about the timeout
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	addr := n.GetAddress()

	n.sync.Lock()
	n.rumorNum += 1
	rm := types.RumorsMessage{
		Rumors: []types.Rumor{
			{
				Origin:   addr,
				Msg:      &msg,
				Sequence: n.rumorNum,
			},
		},
	}
	n.sync.Unlock()

	pkt, err := n.TypeMessageToPacket(rm, addr, addr, addr, 0)
	if err != nil {
		log.Warn().Str("by", addr).Err(err).Msg("packing the rumors message")
		return err
	}

	// use HandlePkt instead of Registry.ProcessesPacket to have the packet
	// logged as any other handled packet
	err = n.HandlePkt(pkt)
	if err != nil {
		return err
	}

	go n.sendRumorsMessage(rm)

	return nil
}

// The routine that handles sending the RumorsMessage to a random neighbor, in a
// loop, until a neighbor sends an ack
func (n *node) sendRumorsMessage(msg types.RumorsMessage) {
	addr := n.GetAddress()
	neighbors := n.routingTable.NeighborsCopy()
	nb := len(neighbors)

	n.sync.Lock()
	if n.rt == nil {
		n.sync.Unlock()
		return
	}
	done := n.rt.context.Done()
	n.sync.Unlock()

	// If there is a timeout, use a real time.Timer
	// Otherwise, create a channel that will never be used
	timer := time.NewTimer(n.conf.AckTimeout)
	if !timer.Stop() {
		<-timer.C
	}

	var timerc <-chan time.Time
	if n.conf.AckTimeout == 0 {
		timerc = make(chan time.Time)
	} else {
		timerc = timer.C
	}

	recvack := make(chan interface{})
	// the list of packeds id we sent containing the rumors
	sent := make([]string, 0)
	// remove the packed id added to the map at the end
	defer func() {
		n.asyncNotifier.RemoveChannel(sent...)
	}()

	for nb != 0 {
		// select a random neighbor
		// it will be the destination of the rumors
		pos := rand.Intn(nb)
		dest := neighbors[pos]

		// remove the selected neighbor
		neighbors[pos] = neighbors[nb-1]
		neighbors = neighbors[:nb-1]
		nb -= 1

		sendpkt, err := n.TypeMessageToPacket(msg, addr, addr, dest, 0)
		if err != nil {
			log.Warn().
				Str("by", addr).
				Str("to", dest).
				Err(err).
				Msg("packing the rumors message")
			continue
		}

		log.Debug().Str("by", addr).Str("to", dest).Msg("send rumors message")
		n.asyncNotifier.AddChannel(sendpkt.Header.PacketID, recvack)
		sent = append(sent, sendpkt.Header.PacketID)
		n.PushSend(sendpkt, dest)

		if n.conf.AckTimeout != 0 {
			timer.Reset(n.conf.AckTimeout)
		}

		select {
		case <-timerc:
			// timed out...
			log.Debug().
				Str("by", addr).
				Str("expected from", dest).
				Msg("ack timeout")
		case from := <-recvack:
			// ack received !
			log.Debug().Str("by", addr).Str("from", from.(string)).Msg("ack received")
			return
		case <-done:
			return
		}
	}
}

// Pushes the packet on the send queue, to be sent to dest.
func (n *node) PushSend(pkt transport.Packet, dest string) {
	n.sync.Lock()
	defer n.sync.Unlock()

	if n.rt == nil {
		return
	}

	n.rt.queueSend.Push(Msg{pkt: pkt, dest: dest})
}

// Send a status message to a random neighbor other than the given one.
// Returns an error if there is none.
func (n *node) SendStatusMessageBut(but string) error {
	dest, err := n.routingTable.GetRandomNeighborBut(but)
	if err != nil {
		return nil
	}
	return n.SendStatusMessageTo(dest)
}

// Send a status message to a random neighbor.
// Returns an error if there is none.
func (n *node) SendStatusMessage() error {
	dest, err := n.routingTable.GetRandomNeighbor()
	if err != nil {
		return nil
	}
	return n.SendStatusMessageTo(dest)
}

// Send a status message to the given neighbor.
func (n *node) SendStatusMessageTo(dest string) error {
	addr := n.GetAddress()
	sendpkt, err := n.TypeMessageToPacket(n.status.Copy(), addr, addr, dest, 0)
	if err != nil {
		log.Warn().
			Str("by", addr).
			Str("to", dest).
			Err(err).
			Msg("packing the status message")
		return err
	}
	log.Debug().Str("by", addr).Str("to", dest).Msg("send current status")
	n.PushSend(sendpkt, dest)
	return nil
}

func (n *node) SendSearchRequestMessage(dests []string, origin, id, regexp string, budget uint) {
	size := uint(len(dests))
	addr := n.GetAddress()
	for pos, peer := range dests {
		qte := budget / size
		if uint(pos) < (budget % size) {
			qte++
		}
		msg := types.SearchRequestMessage{
			RequestID: id,
			Origin:    origin,
			Pattern:   regexp,
			Budget:    qte,
		}

		pkt, err := n.TypeMessageToPacket(msg, addr, addr, peer, 0)
		if err != nil {
			log.Warn().Err(err).Msg("search all: creating packet from request message")
		} else {
			n.PushSend(pkt, peer)
		}
	}
}
