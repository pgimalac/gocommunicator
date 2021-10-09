package impl

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
)

// A function generating a Packet.
type PacketGenerator func(time.Duration) (Msg, error)

// A function handling a Packet.
type PacketHandler func(Msg) error

// A small amount of time used to simulate a non-blocking call.
// Used to determine if packets are processed fast enough.
const timedelta time.Duration = time.Microsecond

// A thread pool with auto regulation mechanisms.
// The features of this thread pool are:
// - genericity of how Packets are generated, and handled
// - can specify the core pool size and the max pool size
// - workers are automatically created when there are too much Packets to process,
// and stopped when there are no more packets to process
// - the thread pool can be stopped, workers will then be stopped in a finite amount of time
// and subsequent thread creation will fail
type AutoThreadPool struct {
	generator PacketGenerator
	handler   PacketHandler

	coreSize int
	maxSize  int
	size     int

	name string

	mutex   sync.Mutex
	ttl     time.Duration
	context context.Context
	cancel  context.CancelFunc
}

// Creates a new thread pool with `coreSize` workers, at most `maxSize` workers,
// the given generator and handler functions, and a given time to live for the workers.
// If `coreSize` is negative or zero, `coreSize` is set to 1.
// If `maxSize` is negative or zero, `maxSize` is set to 1.
// If `maxSize` is smaller than `coreSize`, `maxSize` is changed to `coreSize`.
func NewAutoThreadPool(coreSize, maxSize int,
	generator PacketGenerator,
	handler PacketHandler,
	name string,
	ttl time.Duration) *AutoThreadPool {

	if coreSize < 1 {
		coreSize = 1
	}
	if maxSize < 1 {
		maxSize = 1
	}
	if coreSize > maxSize {
		maxSize = coreSize
	}

	log.Info().Str("pool name", name).
		Int("core size", coreSize).
		Int("max size", maxSize).
		Msg("New thread pool created")

	context, cancel := context.WithCancel(context.Background())

	tp := &AutoThreadPool{
		generator: generator,
		handler:   handler,

		coreSize: coreSize,
		maxSize:  maxSize,
		size:     0,

		name: name,

		mutex:   sync.Mutex{},
		ttl:     ttl,
		context: context,
		cancel:  cancel,
	}

	for tp.GetSize() < tp.GetCoreSize() {
		tp.TrySpawnWorker()
	}

	return tp
}

// Returns the size of the thread pool.
func (tp *AutoThreadPool) GetSize() int {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	return tp.size
}

// Returns the max size of the thread pool.
func (tp *AutoThreadPool) GetMaxSize() int {
	return tp.maxSize
}

// Returns the core size of the thread pool.
func (tp *AutoThreadPool) GetCoreSize() int {
	return tp.coreSize
}

// The routine executed by workers.
// A worker
// - attempts to receive a packet, waiting at most `ttl`
// - if it doesn't receive one and there are more workers than the core pool size, it stops
// - if it receives one, it handles the packet
// - after handling a packet, the worker tries to receive one, waiting at most `timedelay`
// - if it gets one, packets aren't handled fast enough and it tries to create a new worker
// - if it doesn't get one, it starts over
func (tp *AutoThreadPool) worker() {
	log.Debug().Str("pool name", tp.name).
		Int("new pool size", tp.GetSize()).
		Msg("start one new worker")

	worked := false

	for {
		if tp.context.Err() != nil {
			tp.mutex.Lock()
			break
		}

		pkt, err := tp.generator(timedelta)
		if worked && err == nil {
			// we just finished handling a Packet, and there is already another one waiting to be handled
			// increase the number of workers
			tp.TrySpawnWorker()
		}
		worked = false

		// if the short-blocking call received a timeout
		// call using ttl timeout
		if errors.Is(err, transport.TimeoutErr(0)) {
			pkt, err = tp.generator(tp.ttl)
		}
		if err != nil {
			if errors.Is(err, transport.TimeoutErr(0)) {
				// in case of timeout, if there are more than the core pool size
				// just stop the worker
				tp.mutex.Lock()
				if tp.size > tp.GetCoreSize() {
					break
				}
				tp.mutex.Unlock()
			} else if tp.context.Err() != nil {
				// some errors receiving a Packet could be due to the generator being stopped too
				tp.mutex.Lock()
				break
			} else if !errors.Is(err, StoppedError{}) {
				log.Warn().Str("pool name", tp.name).Err(err).Msg("")
				//TODO check what kind of error there can be
				// and handle specifically
			}
			continue
		}

		err = tp.handler(pkt)
		if err != nil && !errors.Is(err, StoppedError{}) {
			log.Warn().Str("pool name", tp.name).Err(err).Msg("")
			//TODO check what kind of error there can be
			// and handle specifically
		}
		worked = true
	}

	// le mutex has been locked before exiting the loop
	tp.size--
	log.Debug().Str("pool name", tp.name).Int("new pool size", tp.size).Msg("stop one worker")
	tp.mutex.Unlock()
}

// Attempts to create a new worker, and increases the pool size.
// Nothing will happen if either the thread pool has been stopped,
// or the maximal size has been reached.
func (tp *AutoThreadPool) TrySpawnWorker() {
	if tp.context.Err() != nil {
		return
	}

	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	if tp.size >= tp.maxSize { // can only be equal in theory
		return
	}

	tp.size++
	go tp.worker()
}

// Stops the thread pool.
// Once this function has been called, all workers will stop within
// a finite time, a no new worker will be created.
func (tp *AutoThreadPool) Stop() {
	log.Info().Str("pool name", tp.name).Msg("stop pool thread")

	tp.cancel()
}
