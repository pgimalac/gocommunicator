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

	header := transport.NewHeader(n.GetAddress(), n.GetAddress(), dest, 0) // don't care about ttl for now

	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	return n.conf.Socket.Send(relay, pkt, 0) // for now we don't care about the timeout
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	addr := n.GetAddress()
	pkt := n.TransportMessageToPacket(msg, addr, addr, addr, 0)

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

	// use HandlePkt instead of Registry.ProcessesPacket to have the packet logged as any other handled packet
	err := n.HandlePkt(pkt)
	if err != nil {
		return err
	}

	go n.sendRumorsMessage(rm)

	return nil
}

// The routine that handles sending the RumorsMessage to a random neighbor, in a loop,
// until a neighbor sends an ack
func (n *node) sendRumorsMessage(msg types.RumorsMessage) {
	addr := n.GetAddress()
	neighbors := n.routingTable.NeighborsCopy()
	nb := len(neighbors)
	timer := time.NewTimer(n.conf.AckTimeout)
	if !timer.Stop() {
		<-timer.C
	}

	recvack := make(chan struct{})

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
			log.Warn().Str("by", addr).Str("to", dest).Err(err).Msg("packing the rumors message")
			continue
		}

		n.expectedAcks.AddChannel(sendpkt.Header.PacketID, recvack)
		n.PushSend(sendpkt, dest)

		timer.Reset(n.conf.AckTimeout)
		select {
		case <-timer.C:
			// timed out...
			n.expectedAcks.RemoveChannel(sendpkt.Header.PacketID)
			//TODO we could defer this removal so that if a peer acks after the timeout we still stop
		case <-recvack:
			// ack received !
			n.expectedAcks.RemoveChannel(sendpkt.Header.PacketID)
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
		log.Warn().Str("by", addr).Str("to", dest).Err(err).Msg("packing the status message")
		return err
	}
	log.Debug().Str("by", addr).Str("to", dest).Msg("send current status")
	n.PushSend(sendpkt, dest)
	return nil
}
