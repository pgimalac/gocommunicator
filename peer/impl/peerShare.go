package impl

import (
	"crypto"
	"encoding/hex"
	"io"
	"regexp"
	"strings"
	"time"

	"go.dedis.ch/cs438/peer"
)

func copySlice(sl []byte) []byte {
	cpy := make([]byte, len(sl))
	copy(cpy, sl)
	return cpy
}

func (n *node) Upload(data io.Reader) (string, error) {
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
	return metafileKey, nil
}

// Download will get all the necessary chunks corresponding to the given
// metahash that references a blob, and return a reconstructed blob. The
// peer will save locally the chunks that it doesn't have for further
// sharing. Returns an error if it can't get the necessary chunks.
func (n *node) Download(metahash string) ([]byte, error) {
	//TODO
	return nil, nil
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
