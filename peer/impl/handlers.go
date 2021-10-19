package impl

// the functions defined in this file handle all kinds of packets

import (
	"sort"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

func (n *node) HandleMsg(msg Msg) error {
	return n.HandlePkt(msg.pkt)
}

func (n *node) HandlePkt(pkt transport.Packet) error {
	addr := n.GetAddress()

	log.Info().
		Str("by", addr).
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Msg("handle packet")

	if pkt.Header.Destination == addr {
		return n.conf.MessageRegistry.ProcessPacket(pkt)
	}

	pkt.Header.RelayedBy = addr
	return n.conf.Socket.Send(pkt.Header.Destination, pkt, 0) // for now we don't care about the timeout
}

func (n *node) HandleChatmessage(msg types.Message, pkt transport.Packet) error {
	// already logged when received
	return nil
}

func (n *node) HandleRumorsMessage(msg types.Message, pkt transport.Packet) error {
	addr := n.GetAddress()
	ack := types.AckMessage{AckedPacketID: pkt.Header.PacketID, Status: n.status.Copy()}
	ackpkt, err := n.TypeMessageToPacket(ack, addr, addr, pkt.Header.RelayedBy, 0)
	if err != nil {
		return err
	}
	if !n.IsNeighbor(pkt.Header.RelayedBy) {
		//TODO check if we add the peer or return an error ?
		n.AddPeer(pkt.Header.RelayedBy)
	}
	n.rt.queueSend.Push(Msg{pkt: ackpkt, dest: pkt.Header.RelayedBy})

	relay := pkt.Header.RelayedBy
	ttl := pkt.Header.TTL

	rumorsmsg := msg.(*types.RumorsMessage)
	isNew := false

	// Sort the rumors by their sequence number, so that dont ignore X+1 then read X
	sort.Slice(rumorsmsg.Rumors, func(i, j int) bool {
		return rumorsmsg.Rumors[i].Sequence < rumorsmsg.Rumors[j].Sequence
	})

	for _, rumor := range rumorsmsg.Rumors {
		if _, ok := n.status.IsNext(rumor.Origin, rumor.Sequence); ok {
			pkt := n.TransportMessageToPacket(*rumor.Msg, rumor.Origin, relay, addr, ttl)
			err = n.HandlePkt(pkt)
			if err != nil {
				return err
			}
			isNew = true
		}
	}

	if isNew {
		dest, err := n.routingTable.GetRandomNeighborBut(pkt.Header.RelayedBy)
		if err != nil {
			return nil
		}
		sendpkt := pkt.Copy()
		sendpkt.Header.RelayedBy = addr
		n.rt.queueSend.Push(Msg{pkt: pkt, dest: dest})
	}

	return nil
}

func (n *node) HandleAckMessage(msg types.Message, pkt transport.Packet) error {
	//TODO
	return nil
}

func (n *node) HandleStatusMessage(msg types.Message, pkt transport.Packet) error {
	//TODO
	return nil
}

func (n *node) HandleEmptyMessage(msg types.Message, pkt transport.Packet) error {
	//TODO
	return nil
}

func (n *node) HandlePrivateMessage(msg types.Message, pkt transport.Packet) error {
	//TODO
	return nil
}
