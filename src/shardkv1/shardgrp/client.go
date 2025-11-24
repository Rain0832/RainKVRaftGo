package shardgrp

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt       *tester.Clnt
	servers    []string
	leaderHint int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) callServer(i int, rpcname string, args any, reply any) bool {
	server := ck.servers[i]
	ok := ck.clnt.Call(server, rpcname, args, reply)
	return ok
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	var reply rpc.GetReply

	log.Printf("shardgrp client: trying to Get(%s) from %d servers", key, len(ck.servers))

	n := len(ck.servers)
	for i := 0; i < n; i++ {
		idx := (ck.leaderHint + i) % n
		server := ck.servers[idx]
		log.Printf("shardgrp client: calling %s for Get(%s)", server, key)
		ok := ck.callServer(idx, "KVServer.Get", args, &reply)
		if ok {
			log.Printf("shardgrp client: Get(%s) from %s returned %v", key, server, reply.Err)
			if reply.Err == rpc.OK {
				ck.leaderHint = idx
				return reply.Value, reply.Version, reply.Err
			}
		} else {
			log.Printf("shardgrp client: Get(%s) from %s failed (network error)", key, server)
		}
	}
	log.Printf("shardgrp client: Get(%s) failed from all servers", key)
	return "", 0, rpc.ErrWrongLeader
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	var reply rpc.PutReply
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		idx := (ck.leaderHint + i) % n
		ok := ck.callServer(idx, "KVServer.Put", args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderHint = idx
			return reply.Err
		}
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := &shardrpc.FreezeShardArgs{Shard: s, Num: num}
	var reply shardrpc.FreezeShardReply
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		idx := (ck.leaderHint + i) % n
		ok := ck.callServer(idx, "KVServer.FreezeShard", args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderHint = idx
			return reply.State, reply.Err
		}
	}
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.InstallShardArgs{State: state, Shard: s, Num: num}
	var reply shardrpc.InstallShardReply
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		idx := (ck.leaderHint + i) % n
		ok := ck.callServer(idx, "KVServer.InstallShard", args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderHint = idx
			return reply.Err
		}
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.DeleteShardArgs{Shard: s, Num: num}
	var reply shardrpc.DeleteShardReply
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		idx := (ck.leaderHint + i) % n
		ok := ck.callServer(idx, "KVServer.DeleteShard", args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderHint = idx
			return reply.Err
		}
	}
	return rpc.ErrWrongLeader
}
