package impl

import (
	"sync"

	"go.dedis.ch/cs438/peer"
)

// An implementation of a catalog.
// Associates to a metahash a set of peer that hold
// the content of the file.
type SafeCatalog struct {
	catalog peer.Catalog
	sync    sync.Mutex
}

// Creates a new empty SafeCatalog.
func NewSafeCatalog() SafeCatalog {
	return SafeCatalog{
		catalog: make(peer.Catalog),
	}
}

// Adds the given addr in the set of peers holding the content of the file.
func (sc *SafeCatalog) Put(key, addr string) {
	sc.sync.Lock()
	defer sc.sync.Unlock()

	set, ok := sc.catalog[key]
	if !ok {
		set = make(map[string]struct{})
		sc.catalog[key] = set
	}
	set[addr] = struct{}{}
}

// Returns a copy of the underlying peer.Catalog.
func (sc *SafeCatalog) Copy() peer.Catalog {
	sc.sync.Lock()
	defer sc.sync.Unlock()

	cpy := make(peer.Catalog)
	for key, set := range sc.catalog {
		cpy[key] = make(map[string]struct{})
		for addr := range set {
			cpy[key][addr] = struct{}{}
		}
	}

	return cpy
}

// Returns the list of peers that hold the content of the file.
// Returns an empty slice if there is none, or if the metahash is unknown.
func (sc *SafeCatalog) Get(key string) []string {
	sc.sync.Lock()
	defer sc.sync.Unlock()

	set, ok := sc.catalog[key]
	if !ok {
		return []string{}
	}

	dests := make([]string, 0, len(set))
	for key := range set {
		dests = append(dests, key)
	}
	return dests
}

// Removes the given peer from the set of the given metahash.
func (sc *SafeCatalog) Remove(key, addr string) {
	sc.sync.Lock()
	defer sc.sync.Unlock()

	set, ok := sc.catalog[key]
	if ok {
		delete(set, addr)
	}
}
