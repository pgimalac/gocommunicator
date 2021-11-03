package impl

// the functions defined in this file handle all kinds of packets

import (
	"errors"
	"math/rand"
	"regexp"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
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
	if pkt.Header.TTL == 1 {
		return nil
	}
	if pkt.Header.TTL != 1 {
		pkt.Header.TTL--
	}

	relay, ok := n.routingTable.GetRelay(pkt.Header.Destination)
	if !ok {
		return errors.New("cannot relay the packet: no relay for the given destination")
	}

	n.PushSend(pkt, relay)
	return nil
}

func (n *node) HandleChatmessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	// already logged when received
	return nil
}

func (n *node) HandleRumorsMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	addr := n.GetAddress()
	log.Debug().Str("by", addr).Msg("handle rumors message")

	relay := pkt.Header.RelayedBy
	ttl := pkt.Header.TTL

	rumorsmsg := msg.(*types.RumorsMessage)
	isNew := false

	// Sort the rumors by their sequence number, so that we dont ignore X+1
	// and then read X
	sort.Slice(rumorsmsg.Rumors, func(i, j int) bool {
		return rumorsmsg.Rumors[i].Sequence < rumorsmsg.Rumors[j].Sequence
	})

	for _, rumor := range rumorsmsg.Rumors {
		if _, ok := n.status.ProcessRumor(rumor); ok {
			if !n.IsNeighbor(rumor.Origin) {
				n.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)
			}
			pkt := n.TransportMessageToPacket(
				*rumor.Msg,
				rumor.Origin,
				relay,
				addr,
				ttl,
			)
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

	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        n.status.Copy(),
	}
	ackpkt, err := n.TypeMessageToPacket(
		ack,
		addr,
		addr,
		pkt.Header.RelayedBy,
		0,
	)
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

	// signal that an ack was received
	n.expectedAcks.Notify(pkt.Header.PacketID, pkt.Header.Source)

	statusPkt, err := n.TypeMessageToPacket(
		ack.Status,
		pkt.Header.Source,
		pkt.Header.RelayedBy,
		addr,
		0,
	)
	if err != nil {
		log.Warn().Str("by", addr).Err(err).Msg("packing a status message")
	}

	return n.HandlePkt(statusPkt)
}

func (n *node) HandleStatusMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	status := *msg.(*types.StatusMessage)
	addr := n.GetAddress()
	log.Debug().Str("by", addr).Msg("handle status message")

	sendStatus := false
	sendRumors := make([]types.Rumor, 0)

	mystatus := n.status.Copy()
	// Add the peers we have but the other peer doesn't
	// It makes it easier to find what they lack
	for peer := range mystatus {
		if _, ok := status[peer]; !ok {
			status[peer] = 0
		}
	}

	for peer, num := range status {
		mynum := n.status.GetLastNum(peer)

		sendStatus = sendStatus || num > mynum
		if num < mynum {
			log.Debug().
				Str("by", addr).
				Str("from", peer).
				Str("to", pkt.Header.Source).
				Msg("sending packets")
			sendRumors = n.status.AppendRumorsTo(peer, sendRumors, num+1)
		}
	}

	if sendStatus {
		log.Debug().
			Str("by", addr).
			Str("to", pkt.Header.Source).
			Msg("send status to request missing")
		n.SendStatusMessageTo(pkt.Header.Source)
	}

	if len(sendRumors) > 0 {
		rumors := types.RumorsMessage{Rumors: sendRumors}
		sendpkt, err := n.TypeMessageToPacket(
			rumors,
			addr,
			addr,
			pkt.Header.Source,
			0,
		)
		if err != nil {
			log.Warn().
				Str("by", addr).
				Err(err).
				Msg("packing the rumors message")
		} else {
			n.PushSend(sendpkt, pkt.Header.Source)
		}
	}

	if !sendStatus && len(sendRumors) == 0 &&
		rand.Float64() < n.conf.ContinueMongering {
		log.Debug().
			Str("by", addr).
			Str("but", pkt.Header.Source).
			Msg("send status to random neighbor")
		n.SendStatusMessageBut(pkt.Header.Source)
	}

	return nil
}

func (*node) HandleEmptyMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) HandlePrivateMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	priv := msg.(*types.PrivateMessage)

	if _, ok := priv.Recipients[n.GetAddress()]; ok {
		pkt := n.TransportMessageToPacket(
			*priv.Msg,
			pkt.Header.Source,
			pkt.Header.RelayedBy,
			n.GetAddress(),
			0,
		)
		n.HandlePkt(pkt)
	}

	return nil
}

func (n *node) HandleDataRequestMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	addr := n.GetAddress()
	req := msg.(*types.DataRequestMessage)
	value := n.conf.Storage.GetDataBlobStore().Get(req.Key)

	log.Debug().
		Str("by", n.GetAddress()).
		Str("key", req.Key).
		Bool("chunk known", value != nil).
		Msg("handle data request message")

	rep := types.DataReplyMessage{
		RequestID: req.RequestID,
		Key:       req.Key,
		Value:     value,
	}
	repPkt, err := n.TypeMessageToPacket(rep, addr, addr, pkt.Header.Source, 0)
	if err != nil {
		return err
	}

	relay, ok := n.routingTable.GetRelay(pkt.Header.Source)
	if !ok {
		return errors.New("cannot send message to " + pkt.Header.Source + ", no known relay")
	}

	n.PushSend(repPkt, relay)

	return nil
}

func (n *node) HandleDataReplyMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	rep := msg.(*types.DataReplyMessage)
	containsChunk := rep.Value != nil && len(rep.Value) != 0
	log.Debug().
		Str("by", n.GetAddress()).
		Str("key", rep.Key).
		Bool("contains value", containsChunk).
		Msg("handle data reply message")

	if containsChunk {
		n.conf.Storage.GetDataBlobStore().Set(rep.Key, rep.Value)
		n.expectedAcks.Notify(rep.RequestID, pkt.Header.Source)
	}

	return nil
}

// Returns a FileInfo struct for with the given name and metahash.
// The hash is given (while it could be retrieved from the NamingStore)
// to avoid a deadlock, since this function is called from NamingStore.forEach
func (n *node) getFileInfo(name string, hash string) (types.FileInfo, bool) {
	dataBlobStore := n.conf.Storage.GetDataBlobStore()
	metahash := dataBlobStore.Get(hash)
	if metahash == nil {
		return types.FileInfo{}, false
	}
	chunkHashes := strings.Split(string(metahash), peer.MetafileSep)
	chunks := make([][]byte, len(chunkHashes))
	for pos, chunkHash := range chunkHashes {
		if dataBlobStore.Get(chunkHash) != nil {
			chunks[pos] = []byte(chunkHash)
		}
	}

	return types.FileInfo{
		Name:     name,
		Metahash: hash,
		Chunks:   chunks,
	}, true
}

func (n *node) HandleSearchRequestMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	req := msg.(*types.SearchRequestMessage)
	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Str("request id", req.RequestID).
		Str("origin", req.Origin).
		Str("pattern", req.Pattern).
		Uint("budget", req.Budget).
		Msg("handle search request message")

	if n.requestIds.Add(req.RequestID) {
		// the request was already processed before
		return nil
	}

	req.Budget--
	if req.Budget != 0 {
		dests := n.routingTable.GetRandomNeighborsBut(req.Origin, req.Budget)
		n.SendRequestMessage(dests, req.Origin, req.RequestID, req.Pattern, req.Budget)
	}

	regexp := regexp.MustCompile(req.Pattern)
	responses := make([]types.FileInfo, 0)
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if regexp.MatchString(key) {
			fileInfo, ok := n.getFileInfo(key, string(val))
			if ok {
				responses = append(responses, fileInfo)
			}
		}
		return true
	})

	reqmsg := types.SearchReplyMessage{
		RequestID: req.RequestID,
		Responses: responses,
	}

	//TODO check if we answer directly to req.Origin or pkt.RelayBy
	// or using routing table ?
	reqpkt, err := n.TypeMessageToPacket(reqmsg, addr, addr, req.Origin, 0)
	if err != nil {
		return err
	}
	n.PushSend(reqpkt, pkt.Header.RelayedBy)

	return nil
}

func (n *node) HandleSearchReplyMessage(
	msg types.Message,
	pkt transport.Packet,
) error {
	rep := msg.(*types.SearchReplyMessage)
	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Str("request id", rep.RequestID).
		Int("number of match", len(rep.Responses)).
		Msg("handle search reply message")

	// dataBlobStore := n.conf.Storage.GetDataBlobStore()
	for _, info := range rep.Responses {
		n.conf.Storage.GetNamingStore().Set(info.Name, []byte(info.Metahash))
		n.catalog.Put(string(info.Metahash), pkt.Header.Source)
		for _, chunk := range info.Chunks {
			if chunk != nil {
				n.catalog.Put(string(chunk), pkt.Header.Source)
			}
		}
	}
	n.expectedAcks.Notify(rep.RequestID, addr)

	return nil
}
