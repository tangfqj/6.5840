package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key      string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, clientID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			e := lk.ck.Put(lk.key, lk.clientID, 0)
			if e == rpc.OK {
				return
			}
			if e == rpc.ErrMaybe {
				v, _, e2 := lk.ck.Get(lk.key)
				if e2 == rpc.OK && v == lk.clientID {
					return
				}
			}
			continue
		}

		if err != rpc.OK {
			continue
		}

		if val == "" {
			e := lk.ck.Put(lk.key, lk.clientID, ver)
			if e == rpc.OK {
				return
			}
			if e == rpc.ErrMaybe {
				v, _, e2 := lk.ck.Get(lk.key)
				if e2 == rpc.OK && v == lk.clientID {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err != rpc.OK {
			continue
		}

		if val == lk.clientID {
			e := lk.ck.Put(lk.key, "", ver)
			if e == rpc.OK {
				return
			}
			if e == rpc.ErrMaybe {
				v, _, e2 := lk.ck.Get(lk.key)
				if e2 == rpc.OK && v == lk.clientID {
					return
				}
			}
		} else {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}
