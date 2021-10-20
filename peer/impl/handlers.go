package impl

// the functions defined in this file handle all kinds of packets

import (
	"math/rand"
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
	log.Debug().Str("by", addr).Msg("handle rumors message")

	relay := pkt.Header.RelayedBy
	ttl := pkt.Header.TTL

	rumorsmsg := msg.(*types.RumorsMessage)
	isNew := false

	// Sort the rumors by their sequence number, so that dont ignore X+1 then read X
	sort.Slice(rumorsmsg.Rumors, func(i, j int) bool {
		return rumorsmsg.Rumors[i].Sequence < rumorsmsg.Rumors[j].Sequence
	})

	for _, rumor := range rumorsmsg.Rumors {
		if _, ok := n.status.ProcessRumor(rumor); ok {
			pkt := n.TransportMessageToPacket(*rumor.Msg, rumor.Origin, relay, addr, ttl)
			err := n.HandlePkt(pkt)
			if err != nil {
				log.Warn().Err(err).Msg("packing the rumor message")
			}
			isNew = true
		}
	}

	if pkt.Header.Source == addr {
		return nil
	}

	ack := types.AckMessage{AckedPacketID: pkt.Header.PacketID, Status: n.status.Copy()}
	ackpkt, err := n.TypeMessageToPacket(ack, addr, addr, pkt.Header.RelayedBy, 0)
	if err != nil {
		return err
	}

	// Don't add the peer nor return an error ? just send the ack blindly ?
	// if !n.IsNeighbor(pkt.Header.RelayedBy) {
	// 	//TODO check if we add the peer or return an error ?
	// 	n.AddPeer(pkt.Header.RelayedBy)
	// }
	n.PushSend(ackpkt, pkt.Header.RelayedBy)

	if !n.IsNeighbor(pkt.Header.Source) {
		n.SetRoutingEntry(pkt.Header.Source, pkt.Header.RelayedBy)
	}

	if isNew {
		dest, err := n.routingTable.GetRandomNeighborBut(pkt.Header.RelayedBy)
		if err != nil {
			return nil
		}
		sendpkt := pkt.Copy()
		sendpkt.Header.RelayedBy = addr
		sendpkt.Header.Destination = dest
		n.PushSend(sendpkt, dest)
	}

	return nil
}

func (n *node) HandleAckMessage(msg types.Message, pkt transport.Packet) error {
	addr := n.GetAddress()
	ack := msg.(*types.AckMessage)
	log.Debug().Str("by", addr).Msg("handle ack message")

	// check if the ack is expected
	// and if so signal that an ack was received
	if ch := n.expectedAcks.GetChannel(ack.AckedPacketID); ch != nil {
		ch <- pkt.Header.Source
	}

	statusPkt, err := n.TypeMessageToPacket(ack.Status, pkt.Header.Source, pkt.Header.RelayedBy, addr, 0)
	if err != nil {
		log.Warn().Str("by", addr).Err(err).Msg("packing a status message")
	}

	return n.HandlePkt(statusPkt)
}

func (n *node) HandleStatusMessage(msg types.Message, pkt transport.Packet) error {
	status := *msg.(*types.StatusMessage)
	addr := n.GetAddress()
	log.Debug().Str("by", addr).Msg("handle status message")

	sendStatus := false
	sendRumors := make([]types.Rumor, 0)

	for peer, num := range status {
		mynum := n.status.GetLastNum(peer)

		sendStatus = sendStatus || num > mynum
		if num < mynum {
			log.Debug().Str("by", addr).Str("from", peer).Str("to", pkt.Header.Source).Msg("sending packets")
			sendRumors = n.status.AppendRumorsTo(peer, sendRumors, num+1)
		}
	}

	if sendStatus {
		log.Debug().Str("by", addr).Str("to", pkt.Header.Source).Msg("send status to request missing")
		n.SendStatusMessageTo(pkt.Header.Source)
	}

	if len(sendRumors) > 0 {
		rumors := types.RumorsMessage{Rumors: sendRumors}
		sendpkt, err := n.TypeMessageToPacket(rumors, addr, addr, pkt.Header.Source, 0)
		if err != nil {
			log.Warn().Str("by", addr).Err(err).Msg("packing the rumors message")
		} else {
			n.PushSend(sendpkt, pkt.Header.Source)
		}
	}

	if !sendStatus && sendRumors == nil && rand.Float64() < n.conf.ContinueMongering {
		log.Debug().Str("by", addr).Msg("send status to random neighbor")
		n.SendStatusMessageBut(pkt.Header.Source)
	}

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
