package impl

// the functions defined in this file handle all kinds of packets

import (
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
