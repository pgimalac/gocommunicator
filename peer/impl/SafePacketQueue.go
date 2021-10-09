package impl

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

// The size of the buffered outgoing packet channel
const OUTGOING_SIZE int = 100

// A queue with built-in synchronization mechanisms.
// The features of this queue are:
// - a Packet can always be added to the queue without blocking
// - a Packet can be removed from the queue in a blocking way with a timeout
// - a Packet can be removed from the queue without blocking
// - the queue can be stopped, and subsequent add / remove calls will fail
type SafePacketQueue struct {
	packets []*transport.Packet
	out     chan *transport.Packet

	mutex *sync.Mutex
	cond  *sync.Cond

	context context.Context
	cancel  context.CancelFunc
}

type StoppedError struct{}

func (StoppedError) Error() string {
	return "the queue has been stopped"
}

// The routine executed by the packet handler
// Waits for packets to be added to the queue, pops them once by one and pushes them in the outgoing channel
// Stops when the context has been cancelled and after the condition has been broadcasted
// Since it blocks waiting on the condition, it needs to be woken up if the context is cancelled
func (queue *SafePacketQueue) packetHandler() {
	for {
		queue.mutex.Lock()

		// Don't use queue.IsEmpty() to avoid double Lock
		for len(queue.packets) == 0 {
			queue.cond.Wait()
			if queue.context.Err() != nil {
				// the context has been cancelled, exit
				return
			}
		}

		// remove one element from the queue
		pkt := queue.packets[0]
		queue.packets = queue.packets[1:]

		// no need to lock the mutex in this section
		queue.mutex.Unlock()

		sent := false
		for !sent {
			select {
			case queue.out <- pkt:
				// the routine successfully wrote the packet to the outgoing channel
				// we can leave this loop and go wait for another packet
				sent = true
			case <-queue.context.Done():
				// the context has been cancelled, exit
				return
			}
		}
	}
}

// Returns an empty SafePacketQueue.
// Starts the packet handler routine.
func NewSafePacketQueue() *SafePacketQueue {
	mutex := &sync.Mutex{}
	cond := sync.NewCond(mutex)
	context, cancel := context.WithCancel(context.Background())
	out := make(chan *transport.Packet, OUTGOING_SIZE)

	queue := &SafePacketQueue{
		packets: make([]*transport.Packet, 0),
		mutex:   mutex,
		cond:    cond,
		context: context,
		cancel:  cancel,
		out:     out,
	}

	go queue.packetHandler()

	return queue
}

// Returns the size of the queue.
func (queue *SafePacketQueue) GetSize() int {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	return len(queue.packets)
}

// Returns true is the queue is empty and false otherwise.
func (queue *SafePacketQueue) IsEmpty() bool {
	return queue.GetSize() == 0
}

// Adds the given elements at the end of the queue.
// Wakes up the packet handler routine
// If the queue has been stopped, nothing happens and the packet is silently thrown
func (queue *SafePacketQueue) Push(pkt ...*transport.Packet) {
	if queue.context.Err() != nil {
		// the queue has been stopped
		return
	}

	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	queue.packets = append(queue.packets, pkt...)

	queue.cond.Broadcast()
}

// Blocking function to remove an element, with a timeout
// Waits until there is an element in the queue, removes it and returns it.
// If there is no element before the timeout, returns an error.
// If the queue has been stopped, returns an error.
// It is possible that a packet is returned even if the queue has been stopped,
// but this can only happen if Poll was called before the queue was stopped.
func (queue *SafePacketQueue) Pop(timeout time.Duration) (*transport.Packet, error) {
	if queue.context.Err() != nil {
		// the queue has been stopped
		return nil, StoppedError{}
	}

	select {
	case pkt := <-queue.out:
		// we got an element from the outgoing queue
		return pkt, nil
	case <-time.After(timeout):
		// there was no element available before the timeout
		return nil, transport.TimeoutErr(timeout)
	case <-queue.context.Done():
		// the queue has been stopped
		return nil, StoppedError{}
	}
}

// Non-blocking function to remove an element.
// Removes the first element of the queue and returns it.
// Returns an error if the queue is empty.
// Returns an error if the queue has been stopped.
// It is possible that a packet is returned even if the queue has been stopped,
// but this can only happen if Poll was called before the queue was stopped.
func (queue *SafePacketQueue) Poll() (*transport.Packet, error) {
	if queue.context.Err() != nil {
		// the queue has been stopped
		return nil, StoppedError{}
	}

	select {
	case pkt := <-queue.out:
		// we got an element from the outgoing queue
		return pkt, nil
	default:
		// there was no element available
		return nil, errors.New("the queue is empty")
	}
}

// Stops the packet handler thread and all subsequent
func (queue *SafePacketQueue) Stop() {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	// stop the routines
	queue.cancel()

	// Broadcast to wake up the packet handler routine
	// and allow it to notice that the context has been cancelled
	queue.cond.Broadcast()
}
