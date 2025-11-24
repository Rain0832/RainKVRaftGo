package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

const (
	cfgKey  = "shardctrler.current_config"
	nextKey = "shardctrler.next_config"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	/// TODO: Local cache/Lock/...
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	if cfg == nil {
		log.Printf("InitConfig call with nil cfg")
		return
	}
	s := cfg.String()
	log.Printf("InitConfig storing config: %s", s)
	sck.Put(cfgKey, s, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	if new == nil {
		return
	}
	sck.Put(cfgKey, new.String(), 0) /// TODO: TVersion dont know
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	cfgStr, _, er := sck.Get(cfgKey)
	if cfgStr == "" || er == rpc.ErrNoKey {
		cfg := &shardcfg.ShardConfig{
			Num:    0,
			Shards: [shardcfg.NShards]tester.Tgid{},
			Groups: make(map[tester.Tgid][]string),
		}
		for i := range cfg.Shards {
			cfg.Shards[i] = shardcfg.Gid1
		}
		// Default
		cfg.Groups[shardcfg.Gid1] = []string{
			"shardgrp-0-0", "shardgrp-0-1", "shardgrp-0-2",
		}
		return cfg
	}
	cfg := shardcfg.FromString(cfgStr)
	return cfg
}
