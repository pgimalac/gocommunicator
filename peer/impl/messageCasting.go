package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Helper function to get a transport.Message from a types.Message
func (n *node) TypeToTransportMessage(msg types.Message) (transport.Message, error) {
	return n.conf.MessageRegistry.MarshalMessage(msg)
}

// Helper function to get a transport.Packet from a transport.Message and other missing header information
func (n *node) TransportMessageToPacket(msg transport.Message, source, relay, dest string, ttl uint) transport.Packet {
	header := transport.NewHeader(source, relay, dest, ttl)
	return transport.Packet{
		Header: &header,
		Msg:    &msg,
	}
}

// Helper function to get a transport.Message from a transport.Packet
func (n *node) PacketToTransportMessage(pkt transport.Packet) transport.Message {
	return *pkt.Msg
}

// Helper function to get a transport.Packet from a types.Message and other missing header information
func (n *node) TypeMessageToPacket(msg types.Message, source, relay, dest string, ttl uint) (transport.Packet, error) {
	tr, err := n.TypeToTransportMessage(msg)
	if err != nil {
		return transport.Packet{}, err
	}
	return n.TransportMessageToPacket(tr, source, relay, dest, ttl), nil
}
