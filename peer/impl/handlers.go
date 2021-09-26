package impl

// the functions defined in this file handle all kinds of packets

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

func handle_packet(n *node, pkt transport.Packet) error {
	log.Info().
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Msg("packet received")

	if pkt.Header.Destination == n.GetAddress() {
		return n.conf.MessageRegistry.ProcessPacket(pkt)
	}

	pkt.Header.RelayedBy = n.GetAddress()
	return n.conf.Socket.Send(pkt.Header.Destination, pkt, 0) // for now we don't care about the timeout
}

func handle_chatmessage(msg types.Message, pkt transport.Packet) error {
	log.Info().
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Msg("chat message received")

	return nil
}
