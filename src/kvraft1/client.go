package kvraft

import (
	"math/rand"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt        *tester.Clnt
	servers     []string
	leaderIndex int
	clerkId     int64
	reqId       int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.servers = append([]string{}, servers...)
	ck.leaderIndex = 0
	ck.clerkId = int64(rand.Int63()) // Randomly generated client ID
	ck.reqId = 0
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	idx := ck.leaderIndex
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIndex = idx
				return reply.Value, reply.Version, reply.Err
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIndex = idx
				return "", 0, reply.Err
			}
		}
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	idx := ck.leaderIndex
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clerkId, ReqId: ck.reqId}
	ck.reqId++
	first := true

	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIndex = idx
				return reply.Err
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIndex = idx
				return rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrVersion {
				if first {
					ck.leaderIndex = idx
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			}
		}
		first = false
		idx = (idx + 1) % len(ck.servers)
	}
}
