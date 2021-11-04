package udp

import (
	"errors"
	"net"
	"os"
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
func (*UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	log.Debug().
		Str("address", address).
		Msg("create socket")

	sock, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, err
	}

	socket := Socket{
		sock: sock,
		ins:  NewSafePacketSlice(),
		outs: NewSafePacketSlice(),
	}

	log.Info().Str("address", socket.GetAddress()).Msg("socket created")

	return &socket, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	sock net.PacketConn
	ins  SafePacketSlice
	outs SafePacketSlice
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	log.Info().Str("address", s.GetAddress()).Msg("close socket")

	return s.sock.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(
	dest string,
	pkt transport.Packet,
	timeout time.Duration,
) error {
	log.Debug().
		Str("by", s.GetAddress()).
		Str("destination", dest).
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Int64("timeout (ms)", timeout.Milliseconds()).
		Msg("send packet")

	if timeout != 0 {
		deadline := time.Now().Add(timeout)
		err := s.sock.SetWriteDeadline(deadline)
		if err != nil {
			return err
		}
		// removes the deadline at the end of the function to avoid the timeout
		// on future calls
		defer s.sock.SetWriteDeadline(time.Time{})
	}

	buffer, err := pkt.Marshal()
	if err != nil {
		return err
	}

	var destaddr *net.UDPAddr
	destaddr, err = net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return err
	}

	//TODO handle a too long packet by splitting it up ?

	var size int
	size, err = s.sock.WriteTo(buffer, destaddr)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = transport.TimeoutErr(timeout)
		}
		return err
	}

	// add the packet we sent to the list of sent packets
	//TODO not sure if we only add the packet if it was successfully sent or not
	s.outs.Append(pkt.Copy())

	log.Debug().
		Str("by", s.GetAddress()).
		Str("to", dest).
		Int("size", size).
		Msg("packet sent")

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	log.Debug().
		Str("by", s.GetAddress()).
		Int64("timeout (ms)", timeout.Milliseconds()).
		Msg("Recv call")

	if timeout != 0 {
		deadline := time.Now().Add(timeout)
		err := s.sock.SetReadDeadline(deadline)
		if err != nil {
			return transport.Packet{}, err
		}
		// removes the deadline at the end of the function to avoid the timeout
		// on future calls
		defer s.sock.SetReadDeadline(time.Time{})
	}

	buffer := make([]byte, bufSize)
	size, addr, err := s.sock.ReadFrom(buffer)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = transport.TimeoutErr(timeout)
		}
		return transport.Packet{}, err
	}

	var packet transport.Packet
	err = packet.Unmarshal(buffer[:size])
	if err != nil {
		return transport.Packet{}, err
	}

	log.Debug().
		Str("by", s.GetAddress()).
		Str("from", addr.String()).
		Int("size", size).
		Bytes("content", buffer[:size]).
		Msg("packet received")

	s.ins.Append(packet.Copy())

	return packet, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	// log.Debug().Msg("get address")

	return s.sock.LocalAddr().String() // or RemoteAddr ?
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	log.Debug().Msg("get received messages")

	return s.ins.Get()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	log.Debug().Msg("get sent messages")

	return s.outs.Get()
}
