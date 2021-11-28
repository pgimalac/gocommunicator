package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
)

// Allocates and returns a copy of the given slice.
func copySlice(sl []byte) []byte {
	return append(make([]byte, 0, len(sl)), sl...)
}

func ChunkEncode(chunk []byte) ([]byte, error) {
	h := crypto.SHA256.New()
	_, err := h.Write(chunk)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func (n *node) Upload(data io.Reader) (string, error) {
	addr := n.GetAddress()

	buff := make([]byte, n.conf.ChunkSize)
	dataBlobStore := n.conf.Storage.GetDataBlobStore()

	var len uint = 0
	var err error = nil
	sz := 0

	// metafileValue holds the hex-encoded SHA256 hashes of the chunks
	// separated by the separator
	metafileValue := ""
	// metafileKeyBuff holds the concatenation of the SHA256 hashes of the chunks
	metafileKeyBuff := make([]byte, 0)

	for err == nil {
		sz, err = data.Read(buff[len:])

		if err != nil && err != io.EOF {
			return "", err
		}

		len += uint(sz)
		if len != 0 && (len == n.conf.ChunkSize || err != nil) {
			chunkHash, err := ChunkEncode(buff[:len])
			if err != nil {
				return "", err
			}

			chunkStrHash := hex.EncodeToString(chunkHash)
			metafileKeyBuff = append(metafileKeyBuff, chunkHash...)

			dataBlobStore.Set(chunkStrHash, copySlice(buff[:len]))
			log.Debug().Str("by", addr).Bytes("chunk hash", chunkHash).Msg("add chunk to data blob store")

			metafileValue += chunkStrHash
			metafileValue += peer.MetafileSep
			len = 0
		}
	}

	// Remove the separator at the end (if there is one)
	metafileValue = strings.TrimSuffix(metafileValue, peer.MetafileSep)
	metafileHash, err := ChunkEncode(metafileKeyBuff)
	if err != nil {
		return "", err
	}
	metafileKey := hex.EncodeToString(metafileHash)
	dataBlobStore.Set(metafileKey, copySlice([]byte(metafileValue)))
	log.Info().Str("by", addr).Str("metafile key", metafileKey).Msg("upload data")

	return metafileKey, nil
}

// Concatenates the chunks of the given file
// and returns the original file
// If the metahash is unknown or any chunk is missing,
// returns an error
func (n *node) getFile(metahash string) (file []byte, err error) {
	addr := n.GetAddress()
	defer func() {
		// log the return status of this call
		log.Debug().
			Str("by", addr).
			Str("metahash", metahash).
			Bool("success", err != nil).
			Msg("get file from local storage")
	}()

	file = make([]byte, 0)
	dataBlobStore := n.conf.Storage.GetDataBlobStore()
	metafileValue := dataBlobStore.Get(metahash)
	if metafileValue == nil {
		return nil, errors.New("unknown metahash")
	}

	chunksHashes := strings.Split(string(metafileValue), peer.MetafileSep)
	for _, chunkHash := range chunksHashes {
		chunk := dataBlobStore.Get(chunkHash)
		if chunk == nil {
			return nil, errors.New("there is a missing chunk")
		}

		file = append(file, chunk...)
	}
	return file, nil
}

// Requests the given chunk from the given peer.
// Uses an exponential backoff, with parameters from the config.
// If the peer sends back the chunk, returns the chunk, otherwise an error.
// The possibles causes of error are:
// - the packet cannot be created
// - there is no relay for the given destination peer
// - the peer doesn't have the chunk (the peer is then removed from the catalog)
// - the node is stopped
// - the backoff expires
func (n *node) requestChunk(dest, hash string) ([]byte, error) {
	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Str("destination", dest).
		Str("metahash / chunk hash", hash).
		Msg("request chunk from peer")

	id := xid.New().String()
	msg := types.DataRequestMessage{
		RequestID: id,
		Key:       hash,
	}

	pkt, err := n.TypeMessageToPacket(msg, addr, addr, dest, 0)
	if err != nil {
		return nil, err
	}
	relay, ok := n.routingTable.GetRelay(dest)
	if !ok {
		return nil, errors.New("no way to reach the peer")
	}

	recvack := make(chan interface{})
	n.asyncNotifier.AddChannel(id, recvack)
	defer n.asyncNotifier.RemoveChannel(id)

	done := n.rt.context.Done()
	backoff := n.conf.BackoffDataRequest.Initial
	for try := uint(0); try <= n.conf.BackoffDataRequest.Retry; try++ {
		log.Debug().
			Str("by", addr).
			Dur("backoff", backoff).
			Str("destination", dest).
			Str("metahash / chunk hash", hash).
			Msg("request chunk from peer")

		n.PushSend(pkt, relay)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
			// timed out...
		case <-recvack:
			// ack received !
			// check if the chunk was added
			chunk := n.conf.Storage.GetDataBlobStore().Get(hash)
			if chunk == nil {
				// shouldn't happen but you never know
				n.catalog.Remove(hash, dest)
				return nil, errors.New("the peer doesn't have the chunk")
			}
			return chunk, nil
		case <-done:
			return nil, StoppedError{}
		}
		backoff *= time.Duration(n.conf.BackoffDataRequest.Factor)
	}

	return nil, errors.New("chunk request backoff expired")
}

func (n *node) Download(metahash string) ([]byte, error) {
	addr := n.GetAddress()
	log.Info().
		Str("by", addr).
		Str("metahash", metahash).
		Msg("download")

	// check if the file is not already available in storage
	file, err := n.getFile(string(metahash))
	if err == nil {
		return file, nil
	}

	dests := n.catalog.Get(metahash)
	if len(dests) == 0 {
		return nil, errors.New("unknown file")
	}
	destpos := rand.Intn(len(dests))
	dest := dests[destpos]

	dataBlobStore := n.conf.Storage.GetDataBlobStore()
	metafileValue := dataBlobStore.Get(metahash)
	if metafileValue == nil {
		metafileValue, err = n.requestChunk(dest, metahash)
		if err != nil {
			return nil, err
		}
	}

	chunksHashes := strings.Split(string(metafileValue), peer.MetafileSep)
	file = make([]byte, 0, len(chunksHashes)*int(n.conf.ChunkSize))
	for _, chunkHash := range chunksHashes {
		chunk := dataBlobStore.Get(chunkHash)
		if chunk == nil {
			chunk, err = n.requestChunk(dest, chunkHash)
			if err != nil {
				return nil, err
			}
		}

		file = append(file, chunk...)
	}

	return file, nil
}

func (n *node) Tag(name string, mh string) error {
	n.paxosinfo.runlock.Lock()
	defer n.paxosinfo.runlock.Unlock()

	n.sync.Lock()
	rt := n.rt
	n.sync.Unlock()
	if rt == nil {
		return nil
	}
	context := rt.context

	done := false
	for !done {
		if n.conf.Storage.GetNamingStore().Get(name) != nil {
			return fmt.Errorf("tag: %s already exists in the naming store", name)
		}

		if n.conf.TotalPeers == 1 {
			done = true
		} else {
			validname, validmh, err := n.paxosinfo.Start(n, name, mh)

			if err != nil {
				if context.Err() != nil { // the peer is stopped
					return nil
				}
				return err
			}

			done = validname == name && validmh == mh
			n.conf.Storage.GetNamingStore().Set(validname, []byte(validmh))
		}
	}

	n.conf.Storage.GetNamingStore().Set(name, []byte(mh))

	return nil
}

func (n *node) Resolve(name string) string {
	mh := n.conf.Storage.GetNamingStore().Get(name)
	if mh == nil {
		return ""
	}
	return string(mh)
}

func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.Copy()
}

func (n *node) UpdateCatalog(key string, peer string) {
	log.Debug().
		Str("by", n.GetAddress()).
		Str("file metahash", key).
		Str("peer", peer).
		Msg("update catalog")

	n.catalog.Put(key, peer)
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	addr := n.GetAddress()
	dests := n.routingTable.GetRandomNeighbors(budget)

	id := xid.New().String()
	n.requestIds.Add(id)

	n.SendSearchRequestMessage(dests, addr, id, reg.String(), budget)
	destset := make(map[string]struct{})
	for _, dest := range dests {
		destset[dest] = struct{}{}
	}

	timer := time.NewTicker(timeout)
	<-timer.C

	names := make([]string, 0)
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) {
			names = append(names, key)
		}
		return true
	})

	return names, nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (string, error) {
	addr := n.GetAddress()
	match := ""
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if !pattern.MatchString(key) {
			return true
		}

		_, err := n.getFile(string(val))
		if err == nil {
			match = key
			return false
		}
		return true
	})

	if match != "" {
		return match, nil
	}

	budget := conf.Initial
	ch := make(chan interface{})
	ids := make([]string, 0)
	defer func() {
		n.asyncNotifier.RemoveChannel(ids...)
	}()

	for try := uint(0); try < conf.Retry; try++ {
		timer := time.NewTimer(conf.Timeout)
		id := xid.New().String()
		ids = append(ids, id)
		n.asyncNotifier.AddChannel(id, ch)
		dests := n.routingTable.GetRandomNeighbors(budget)
		n.SendSearchRequestMessage(dests, addr, id, pattern.String(), budget)
		for {
			select {
			case msg := <-ch:
				rep := *msg.(*types.SearchReplyMessage)
				for _, fileinfo := range rep.Responses {
					fullfile := true
					for _, chunk := range fileinfo.Chunks {
						if chunk == nil {
							fullfile = false
							break
						}
					}
					if fullfile {
						return fileinfo.Name, nil
					}
				}
				continue
			case <-timer.C:
			}
			// timeout
			break
		}

		budget *= conf.Factor
	}

	return "", nil
}
