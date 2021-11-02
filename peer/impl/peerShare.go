package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
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

func (n *node) Upload(data io.Reader) (string, error) {
	addr := n.GetAddress()

	buff := make([]byte, n.conf.ChunkSize)
	h := crypto.SHA256.New()
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
			_, wrerr := h.Write(buff[:len])
			if wrerr != nil {
				return "", wrerr
			}

			chunkHash := h.Sum(nil)
			chunkStrHash := hex.EncodeToString(chunkHash)
			metafileKeyBuff = append(metafileKeyBuff, chunkHash...)

			dataBlobStore.Set(chunkStrHash, copySlice(buff[:len]))
			log.Debug().Str("by", addr).Bytes("chunk hash", chunkHash).Msg("add chunk to data blob store")

			metafileValue += chunkStrHash
			metafileValue += peer.MetafileSep
			h.Reset()
			len = 0
		}
	}

	// Remove the separator at the end (if there is one)
	metafileValue = strings.TrimSuffix(metafileValue, peer.MetafileSep)
	_, wrerr := h.Write(metafileKeyBuff)
	if wrerr != nil {
		return "", wrerr
	}
	metafileKey := hex.EncodeToString(h.Sum(nil))
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

	recvack := make(chan string)
	n.expectedAcks.AddChannel(id, recvack)
	defer n.expectedAcks.RemoveChannel(id)

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
	//TODO possible optimization: change destination when timeout
	// or use several in parallel
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

// Tag creates a mapping between a (file)name and a metahash.
func (n *node) Tag(name string, mh string) error {
	//TODO
	return nil
}

// Resolve returns the corresponding metahash of a given (file)name. Returns
// an empty string if not found.
func (n *node) Resolve(name string) string {
	//TODO
	return ""
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

// SearchAll returns all the names that exist matching the given regex. It
// merges results from the local storage and from the search request reply
// sent to a random neighbor using the provided budget. It makes the peer
// update its catalog and name storage according to the SearchReplyMessages
// received. Returns an empty result if nothing found. An error is returned
// in case of an exceptional event.
func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	//TODO
	return nil, nil
}

// SearchFirst uses an expanding ring configuration and returns a name as
// soon as it finds a peer that "fully matches" a data blob. It makes the
// peer update its catalog and name storage according to the
// SearchReplyMessages received. Returns an empty string if nothing was
// found.
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	//TODO
	return "", nil
}
