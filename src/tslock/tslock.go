package tslock

import (
	"sync"
	"time"
)

type TsLock struct {
	wlockN uint64
	w      uint64
	rlockN uint64
	mutex0 sync.Mutex
	mutex1 sync.Mutex
}

func NewTsLock() *TsLock {
	return &TsLock{}
}

func (lock *TsLock) TryLock(t time.Duration, id uint64) bool {
	timeout := time.After(t)
	for {
		lock.mutex0.Lock()
		if lock.w == 0 {
			lock.w = id
			lock.wlockN++
			break
		} else if lock.w == id {
			lock.wlockN++
			break
		}
		lock.mutex0.Unlock()
		select {
		case <-timeout:
			return false
		default:
			time.Sleep(time.Millisecond * 1)
			continue
		}
	}
	lock.mutex0.Unlock()
	return true
}

func (lock *TsLock) TryRLock(t time.Duration, id uint64) bool {
	timeout := time.After(t)
	for {
		if lock.TryLock(0, id) {
			lock.mutex1.Lock()
			lock.rlockN++
			lock.mutex1.Unlock()
			break
		}
		lock.mutex1.Lock()
		if lock.rlockN > 0 {
			lock.mutex1.Unlock()
			break
		}
		lock.mutex1.Unlock()
		select {
		case <-timeout:
			return false
		default:
			time.Sleep(time.Millisecond * 1)
			continue
		}
	}
	return true
}

func (lock *TsLock) Cancel() {
	lock.mutex1.Lock()
	if lock.rlockN > 0 {
		lock.rlockN--
	}
	lock.mutex1.Unlock()
	lock.mutex0.Lock()
	if lock.wlockN > 0 {
		lock.wlockN--
		if lock.wlockN == 0 {
			lock.w = 0
		}
	}
	lock.mutex0.Unlock()
}
