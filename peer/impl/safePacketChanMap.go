package impl

import "sync"

type SafePacketChanMap struct {
	sync     sync.Mutex
	channels map[string]chan string
}

func NewSafePacketChanMap() SafePacketChanMap {
	return SafePacketChanMap{
		channels: make(map[string]chan string),
	}
}

func (spcm *SafePacketChanMap) AddChannel(packet string, ch chan string) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	spcm.channels[packet] = ch
}

func (spcm *SafePacketChanMap) RemoveChannel(packet string) {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	delete(spcm.channels, packet)
}

func (spcm *SafePacketChanMap) GetChannel(packet string) chan string {
	spcm.sync.Lock()
	defer spcm.sync.Unlock()

	return spcm.channels[packet]
}
