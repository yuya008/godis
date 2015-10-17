package godis

import (
	_ "log"
	"sync"
	"sync/atomic"
	"time"
)

type TsLock struct {
	w     uint64
	r     uint64
	mutex sync.Mutex
}

func NewTsLock() *TsLock {
	return &TsLock{
		w: 0,
		r: 0,
	}
}

func (lock *TsLock) TryLock(t time.Duration, id uint64) bool {
	timeout := time.After(t)
	for {
		if atomic.CompareAndSwapUint64(&lock.w, 0, id) {
			break
		}
		if atomic.CompareAndSwapUint64(&lock.w, id, id) {
			break
		}
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

// func (lock *TsLock) Unlock(id uint64) {
// 	atomic.CompareAndSwapUint64(&lock.w, id, 0)
// }

func (lock *TsLock) TryRLock(t time.Duration, id uint64) bool {
	timeout := time.After(t)
	for {
		lock.mutex.Lock()
		if lock.TryLock(0, id) {
			atomic.AddUint64(&lock.r, 1)
			lock.mutex.Unlock()
			break
		}
		if atomic.LoadUint64(&lock.r) > 0 {
			lock.mutex.Unlock()
			break
		}
		lock.mutex.Unlock()
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

// func (lock *TsLock) RUnlock(id uint64) {
// lock.mutex.Lock()
// if atomic.AddUint64(&lock.r, ^uint64(0)) == 0 {
// 	lock.Unlock(lock.w)
// }
// lock.mutex.Unlock()
// }
// 全体总解锁

func (lock *TsLock) Cancel(id uint64) {
	lock.mutex.Lock()
	atomic.CompareAndSwapUint64(&lock.w, lock.w, 0)
	atomic.CompareAndSwapUint64(&lock.r, lock.r, 0)
	lock.mutex.Unlock()
}

// func main() {
// 	lock := NewTsLock()
// 	// for i := 0; i < 10; i++ {
// 	go func(u int) {
// 		if lock.TryRLock(0, uint64(1)) {
// 			log.Println(u, "加锁成功")
// 		} else {
// 			log.Println(u, "加锁失败")
// 		}
// 		log.Println(u, "解锁")
// 		time.Sleep(time.Second * 2)
// 		lock.RUnlock(uint64(1))
// 	}(0)
// 	// }
// 	go func(u int) {
// 		if lock.TryLock(time.Second*3, uint64(101)) {
// 			log.Println(u, "加锁成功")
// 		} else {
// 			log.Println(u, "加锁失败")
// 		}
// 	}(1)
// 	time.Sleep(time.Second * 10)
// }
