package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt        *tester.Clnt
	sck         *shardctrler.ShardCtrler
	shardClerks map[tester.Tgid]*shardgrp.Clerk
	clientId    int64
	requestId   int64
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:        clnt,
		sck:         sck,
		shardClerks: make(map[tester.Tgid]*shardgrp.Clerk),
		clientId:    rand.Int63(),
		requestId:   0,
	}
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.requestId++

	for attempts := 0; attempts < 5; attempts++ {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid := cfg.Shards[shard]

		servers, ok := cfg.Groups[gid]
		if !ok || len(servers) == 0 {
			log.Printf("No servers found for gid %d", gid)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var shardClerk *shardgrp.Clerk
		if sc, ok := ck.shardClerks[gid]; ok {
			shardClerk = sc
		} else {
			shardClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.shardClerks[gid] = shardClerk
		}

		value, ver, err := shardClerk.Get(key)
		if err == rpc.ErrWrongGroup {
			log.Printf("ErrWrongGroup for key %s, shard %d, gid %d", key, shard, gid)
			continue
		}
		if err == rpc.ErrWrongLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return value, ver, err
	}
	return "", 0, rpc.ErrWrongLeader
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid := cfg.Shards[shard]

		var shardClerk *shardgrp.Clerk
		if sc, ok := ck.shardClerks[gid]; ok {
			shardClerk = sc
		} else {
			servers := cfg.Groups[gid]
			shardClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.shardClerks[gid] = shardClerk
		}

		err := shardClerk.Put(key, value, 0)
		if err == rpc.ErrWrongGroup {
			continue
		}
		return err
	}
}
