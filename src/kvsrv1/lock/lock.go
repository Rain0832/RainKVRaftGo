package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	l  string
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// id := fmt.Sprintf("%p", ck)
	id := kvtest.RandValue(8) // use a random id for now?
	lk := &Lock{ck: ck, l: l, id: id}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			if lk.ck.Put(lk.l, lk.id, 0) == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			if value == "FREE" {
				if lk.ck.Put(lk.l, lk.id, version) == rpc.OK {
					return
				}
			}
			if value == lk.id {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err != rpc.OK {
			return
		}

		if value == lk.id {
			if lk.ck.Put(lk.l, "FREE", version) == rpc.OK {
				return
			}
		} else {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
