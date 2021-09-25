package udp

import (
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	log.Info().Str("address", address).Msg("create socket")
	panic("to be implemented in HW0")
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	log.Info().Msg("close socket")
	panic("to be implemented in HW0")
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	log.Info().
		Str("destination", dest).
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Int64("timeout (ms)", timeout.Milliseconds()).
		Msg("send packet")
	panic("to be implemented in HW0")
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	log.Info().Int64("timeout (ms)", timeout.Milliseconds()).Msg("receive packet")
	panic("to be implemented in HW0")
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	log.Info().Msg("get address")
	panic("to be implemented in HW0")
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	log.Info().Msg("get received message")
	panic("to be implemented in HW0")
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	log.Info().Msg("get sent messages")
	panic("to be implemented in HW0")
}
