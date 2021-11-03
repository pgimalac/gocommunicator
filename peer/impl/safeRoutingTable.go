package impl

import (
	"errors"
	"math/rand"
	"sync"

	"go.dedis.ch/cs438/peer"
)

type SafeRoutingTable struct {
	// the actual routing table
	table peer.RoutingTable
	// array of actual neighbors
	neighbors []string
	// a map giving the position of a neighbor
	// in the array, for fast removal
	positions map[string]int
	// mutex for synchronization purpose
	sync sync.Mutex
}

// creates a new empty SafeRoutingTable
func NewSafeRoutingTable(address string) SafeRoutingTable {
	return SafeRoutingTable{
		table:     map[string]string{address: address},
		neighbors: []string{address},
		positions: map[string]int{address: 0},
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

// Removes the neighbor at the given position from the neighbor list.
// table.sync must be locked before calling this function.
// table.sync is not Unlocked in this function.
func (table *SafeRoutingTable) removeNeighbor(pos int) {
	l := len(table.neighbors)
	addr := table.neighbors[pos]

	// move the last element to the position of the removed one
	table.neighbors[pos] = table.neighbors[l-1]
	// pop the last element
	table.neighbors = table.neighbors[:l-1]
	// remove its position
	delete(table.positions, addr)
	// and update the position map for the former last neighbor
	table.positions[addr] = pos
}

// Adds the given address in the neighbor list.
// table.sync must be locked before calling this function.
// table.sync is not Unlocked in this function.
func (table *SafeRoutingTable) addNeighbor(addr string) {
	table.positions[addr] = len(table.neighbors)
	table.neighbors = append(table.neighbors, addr)
}

// sets the given relay for the given destination
// if relay is empty, deletes dest from the table
func (table *SafeRoutingTable) SetRoutingEntry(dest, relayAddr string) {
	table.sync.Lock()
	defer table.sync.Unlock()

	// the first neighbor is ourself
	// we don't update it
	if dest == table.neighbors[0] {
		return
	}

	// remove the given peer
	if relayAddr == "" {
		// if the peer was a neighbor, remove it from the neighbor list
		pos, ok := table.positions[dest]
		if ok {
			table.removeNeighbor(pos)
		}

		delete(table.table, dest)
		return
	}

	// update the given peer
	pos, ok := table.positions[dest]
	if ok && relayAddr != dest {
		table.removeNeighbor(pos)
	} else if relayAddr == dest {
		table.addNeighbor(dest)
	}

	table.table[dest] = relayAddr
}

// Returns a random neighbor from the routing table.
// A neighbor is an address that is not our own address,
// and whose relay is itself.
func (table *SafeRoutingTable) GetRandomNeighbor() (string, error) {
	table.sync.Lock()
	selfaddr := table.neighbors[0]
	table.sync.Unlock()

	return table.GetRandomNeighborBut(selfaddr)
}

// Returns a random neighbor from the routing table, except the given one.
// A neighbor is an address that is not our own address,
// and whose relay is itself.
func (table *SafeRoutingTable) GetRandomNeighborBut(
	but string,
) (string, error) {
	table.sync.Lock()
	defer table.sync.Unlock()

	is_in := 0
	pos, ok := table.positions[but]
	if ok && but != table.neighbors[0] {
		is_in = 1
	}

	// the first address is our own address
	if len(table.neighbors) <= 1+is_in {
		return "", errors.New("there is no neighbor")
	}

	index := rand.Intn(len(table.neighbors)-1-is_in) + 1
	if is_in == 1 && index >= pos {
		return table.neighbors[1+index], nil
	}
	return table.neighbors[index], nil
}

func (table *SafeRoutingTable) GetRandomNeighbors(n uint) []string {
	table.sync.Lock()
	addr := table.neighbors[0]
	table.sync.Unlock()

	return table.GetRandomNeighborsBut(addr, n)
}

// Returns a slice with n randomly selected neighbors, except the given one.
// If n is greater than the number of remaining neighbors,
// returns all remaining neighbors
func (table *SafeRoutingTable) GetRandomNeighborsBut(but string, n uint) []string {
	neighbors := table.NeighborsCopy()
	size := uint(len(neighbors))

	// a position of 0 means but is the peer's address
	// which has already been removed from neighbors by NeighborsCopy
	pos, ok := table.positions[but]
	if ok && pos != 0 {
		// since neighbors doesn't contain the peer's address
		// the position is shifted by one
		neighbors[pos-1] = neighbors[size-1]
		size--
		neighbors = neighbors[:size]
	}

	// Remove neighbors until there is at most n left
	for size > n {
		pos := rand.Intn(int(size))
		size--
		neighbors[pos] = neighbors[size]
		neighbors = neighbors[:size]
	}

	return neighbors
}

// Returns a copy of the list of neighbors, except ourselves.
func (table *SafeRoutingTable) NeighborsCopy() []string {
	table.sync.Lock()
	defer table.sync.Unlock()

	neighbors := table.neighbors[1:]
	return append(make([]string, 0, len(neighbors)), neighbors...)
}
