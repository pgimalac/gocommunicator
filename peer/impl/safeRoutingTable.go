package impl

import (
	"sync"

	"go.dedis.ch/cs438/peer"
)

type SafeRoutingTable struct {
	table peer.RoutingTable
	sync  sync.Mutex
}

// creates a new empty SafeRoutingTable
func NewSafeRoutingTable(address string) SafeRoutingTable {
	table := make(peer.RoutingTable)
	table[address] = address

	return SafeRoutingTable{
		table: table,
	}
}

// returns the relay associated with the given destination
func (table *SafeRoutingTable) GetRelay(dest string) (string, bool) {
	table.sync.Lock()
	defer table.sync.Unlock()

	value, exists := table.table[dest]
	return value, exists
}

// returns a copy of the underlying routing table
func (table *SafeRoutingTable) Copy() peer.RoutingTable {
	copiedMap := make(map[string]string)

	table.sync.Lock()
	defer table.sync.Unlock()

	for k, v := range table.table {
		copiedMap[k] = v
	}

	return copiedMap
}

// sets the given relay for the given origin
// if relay is empty, deletes origin from the table
func (table *SafeRoutingTable) SetRoutingEntry(origin, relayAddr string) {
	table.sync.Lock()
	defer table.sync.Unlock()

	if relayAddr == "" {
		delete(table.table, origin)
	} else {
		table.table[origin] = relayAddr
	}
}
