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

	log.Debug().
		Str("by", addr).
		Str("header", pkt.Header.String()).
		Str("message type", pkt.Msg.Type).
		Bytes("payload", pkt.Msg.Payload).
		Msg("handle packet")

	if pkt.Header.Destination == addr {
		return n.conf.MessageRegistry.ProcessPacket(pkt)
	}

	pkt.Header.RelayedBy = addr
	pkt.Header.TTL--
	if pkt.Header.TTL == 0 {
		return nil
	}

	relay, ok := n.routingTable.GetRelay(pkt.Header.Destination)
	if !ok {
		return errors.New("cannot relay the packet: no relay for the given destination")
	}

	log.Debug().
		Str("by", addr).
		Str("type", pkt.Msg.Type).
		Str("destination", pkt.Header.Destination).
		Str("through", relay).
		Msg("relay packet")

	n.PushSend(pkt, relay)
	return nil
}

func (n *node) HandleChatMessage(
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
	relay := pkt.Header.RelayedBy
	ttl := pkt.Header.TTL

	rumorsmsg := msg.(*types.RumorsMessage)
	isNew := false

	log.Info().
		Str("by", addr).
		Str("relayed by", pkt.Header.RelayedBy).
		Msg("handle rumors message")

	// Sort the rumors by their sequence number, so that we dont ignore X+1
	// and then read X
	sort.Slice(rumorsmsg.Rumors, func(i, j int) bool {
		return rumorsmsg.Rumors[i].Sequence < rumorsmsg.Rumors[j].Sequence
	})

	for _, rumor := range rumorsmsg.Rumors {
		if _, ok := n.status.ProcessRumor(rumor); ok {
			log.Debug().
				Str("by", addr).
				Str("rumor origin", rumor.Origin).
				Uint("sequence", rumor.Sequence).
				Str("relayed by", pkt.Header.RelayedBy).
				Str("packet source", pkt.Header.Source).
				Msg("check new rumor")

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

	n.PushSend(ackpkt, pkt.Header.RelayedBy)

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
	log.Info().Str("by", addr).Msg("handle ack message")

	// signal that an ack was received
	n.asyncNotifier.Notify(pkt.Header.PacketID, pkt.Header.Source)

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
	log.Info().Str("by", addr).Msg("handle status message")

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

	log.Info().
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
	log.Info().
		Str("by", n.GetAddress()).
		Str("key", rep.Key).
		Bool("contains value", containsChunk).
		Msg("handle data reply message")

	if containsChunk {
		n.conf.Storage.GetDataBlobStore().Set(rep.Key, rep.Value)
		n.asyncNotifier.Notify(rep.RequestID, pkt.Header.Source)
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
		n.SendSearchRequestMessage(dests, req.Origin, req.RequestID, req.Pattern, req.Budget)
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

	for _, info := range rep.Responses {
		n.conf.Storage.GetNamingStore().Set(info.Name, []byte(info.Metahash))
		n.catalog.Put(string(info.Metahash), pkt.Header.Source)
		for _, chunk := range info.Chunks {
			if chunk != nil {
				n.catalog.Put(string(chunk), pkt.Header.Source)
			}
		}
	}
	n.asyncNotifier.Notify(rep.RequestID, rep)

	return nil
}

func (n *node) HandlePaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	prep := msg.(*types.PaxosPrepareMessage)

	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Str("source", prep.Source).
		Uint("step", prep.Step).
		Uint("id", prep.ID).
		Msg("handle paxos prepare message")

	promise, ok := n.paxosinfo.HandlePrepare(prep)
	if !ok {
		return nil
	}

	trprom, err := n.TypeToTransportMessage(promise)
	if err != nil {
		return err
	}

	privmsg := types.PrivateMessage{
		Recipients: map[string]struct{}{prep.Source: {}},
		Msg:        &trprom,
	}

	trpriv, err := n.TypeToTransportMessage(privmsg)
	if err != nil {
		return err
	}

	err = n.Broadcast(trpriv)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) HandlePaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	prom := msg.(*types.PaxosPromiseMessage)

	addr := n.GetAddress()
	logmsg := log.Info().
		Str("by", addr).
		Uint("accepted id", prom.AcceptedID).
		Uint("step", prom.Step).
		Uint("id", prom.ID)
	if prom.AcceptedValue != nil {
		logmsg = logmsg.Str("unique id", prom.AcceptedValue.UniqID).
			Str("filename", prom.AcceptedValue.Filename).
			Str("metahash", prom.AcceptedValue.Metahash)
	}
	logmsg.Msg("handle paxos promise message")

	//TODO
	return nil
}

func (n *node) HandlePaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	prop := msg.(*types.PaxosProposeMessage)

	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Uint("step", prop.Step).
		Uint("id", prop.ID).
		Str("unique id", prop.Value.UniqID).
		Str("filename", prop.Value.Filename).
		Str("metahash", prop.Value.Metahash).
		Msg("handle paxos propose message")

	if !n.paxosinfo.HandlePropose(prop) {
		return nil
	}

	acc := types.PaxosAcceptMessage{
		Step:  prop.Step,
		ID:    prop.ID,
		Value: prop.Value,
	}
	tracc, err := n.TypeToTransportMessage(acc)
	if err != nil {
		return err
	}

	err = n.Broadcast(tracc)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) HandlePaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	acc := msg.(*types.PaxosAcceptMessage)

	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Uint("step", acc.Step).
		Uint("id", acc.ID).
		Str("unique id", acc.Value.UniqID).
		Str("filename", acc.Value.Filename).
		Str("metahash", acc.Value.Metahash).
		Msg("handle paxos accept message")

	//TODO
	return nil
}

func (n *node) HandleTLCMessage(msg types.Message, pkt transport.Packet) error {
	rep := msg.(*types.TLCMessage)

	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Uint("step", rep.Step).
		Uint("block index", rep.Block.Index).
		Str("unique id", rep.Block.Value.UniqID).
		Str("filename", rep.Block.Value.Filename).
		Str("metahash", rep.Block.Value.Metahash).
		Msg("handle TLC message")

	//TODO
	return nil
}
