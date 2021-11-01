package impl

import (
	"sync"

	"go.dedis.ch/cs438/peer"
)

type SafeCatalog struct {
	catalog peer.Catalog
	sync    sync.Mutex
}

func NewSafeCatalog() SafeCatalog {
	return SafeCatalog{
		catalog: make(peer.Catalog),
	}
}

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
