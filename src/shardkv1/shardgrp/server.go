package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpFreezeShard
	OpInstallShard
	OpDeleteShard
)

type Op struct {
	Type    OpType
	Req     any
	ClerkID int64
	ReqID   int64
}

type ShardState struct {
	Data     map[string]string
	Versions map[string]rpc.Tversion
	Frozen   bool
	LastNum  shardcfg.Tnum // last seen config Num for fencing
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu sync.RWMutex
	// state replicated via rsm.DoOp (so snapshot/restore must cover these)
	Shards     map[shardcfg.Tshid]*ShardState // shardID -> ShardState
	ClientLast map[int64]int64                // client -> last seen req id (dedup)
	ClientRes  map[int64]any
}

func init() {
	labgob.Register(Op{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
}

func (kv *KVServer) initStateIfNil() {
	if kv.Shards == nil {
		kv.Shards = make(map[shardcfg.Tshid]*ShardState)
	}
	if kv.ClientLast == nil {
		kv.ClientLast = make(map[int64]int64)
	}
	if kv.ClientRes == nil {
		kv.ClientRes = make(map[int64]any)
	}
}

func ssPresent(s *ShardState) bool {
	return s != nil && s.Data != nil
}

// DoOp: executed under rsm applier; must mutate replicated state and return reply structs
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.initStateIfNil()

	op := req.(Op)
	switch op.Type {
	case OpGet:
		args := op.Req.(*rpc.GetArgs)
		shid := shardcfg.Key2Shard(args.Key)
		ss, ok := kv.Shards[shid]
		if !ok || !ssPresent(ss) {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		val, ok2 := ss.Data[args.Key]
		if !ok2 {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		ver := ss.Versions[args.Key]
		return rpc.GetReply{Value: val, Version: ver, Err: rpc.OK}

	case OpPut:
		args := op.Req.(*rpc.PutArgs)
		shid := shardcfg.Key2Shard(args.Key)
		ss, ok := kv.Shards[shid]
		if !ok || !ssPresent(ss) {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		// If frozen, reject writes
		if ss.Frozen {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		// dedup by client id + req id
		if prev, ok := kv.ClientLast[args.ClientId]; ok {
			if args.ReqId <= prev {
				if prevRes, ok2 := kv.ClientRes[args.ClientId]; ok2 {
					return prevRes
				}
			}
		}
		// apply put (simple overwrite semantics)
		if ss.Data == nil {
			ss.Data = make(map[string]string)
		}
		if ss.Versions == nil {
			ss.Versions = make(map[string]rpc.Tversion)
		}
		newVer := ss.Versions[args.Key] + 1
		ss.Data[args.Key] = args.Value
		ss.Versions[args.Key] = newVer

		kv.ClientLast[args.ClientId] = args.ReqId
		res := rpc.PutReply{Err: rpc.OK}
		kv.ClientRes[args.ClientId] = res
		return res

	case OpFreezeShard:
		args := op.Req.(*shardrpc.FreezeShardArgs)
		shid := args.Shard
		ss, ok := kv.Shards[shid]
		if !ok {
			// create empty shard state, then freeze
			ss = &ShardState{Data: make(map[string]string), Versions: make(map[string]rpc.Tversion)}
			kv.Shards[shid] = ss
		}
		// fence by Num: only accept if args.Num >= ss.LastNum
		if args.Num < ss.LastNum {
			return shardrpc.FreezeShardReply{State: nil, Num: ss.LastNum, Err: rpc.OK}
		}
		ss.Frozen = true
		ss.LastNum = args.Num

		// return copy of data
		w := new(bytes.Buffer)
		enc := labgob.NewEncoder(w)
		enc.Encode(ss.Data)
		enc.Encode(ss.Versions) // also encode versions so install can restore them

		return shardrpc.FreezeShardReply{State: w.Bytes(), Num: args.Num, Err: rpc.OK}

	case OpInstallShard:
		args := op.Req.(*shardrpc.InstallShardArgs)
		shid := args.Shard
		ss, ok := kv.Shards[shid]
		if !ok {
			ss = &ShardState{Data: make(map[string]string), Versions: make(map[string]rpc.Tversion)}
			kv.Shards[shid] = ss
		}
		// fence by Num
		if args.Num < ss.LastNum {
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}
		// decode args.State []byte into map and versions
		if len(args.State) > 0 {
			var data map[string]string
			var vers map[string]rpc.Tversion
			buf := bytes.NewBuffer(args.State)
			dec := labgob.NewDecoder(buf)
			if dec.Decode(&data) == nil {
				ss.Data = make(map[string]string)
				for k, v := range data {
					ss.Data[k] = v
				}
			} else {
				// decode failed: ensure empty map to be safe
				ss.Data = make(map[string]string)
			}
			if dec.Decode(&vers) == nil {
				ss.Versions = make(map[string]rpc.Tversion)
				for k, v := range vers {
					ss.Versions[k] = v
				}
			} else {
				// if no versions encoded, create empty version map
				if ss.Versions == nil {
					ss.Versions = make(map[string]rpc.Tversion)
				}
			}
		} else {
			// empty state: create empty maps
			if ss.Data == nil {
				ss.Data = make(map[string]string)
			}
			if ss.Versions == nil {
				ss.Versions = make(map[string]rpc.Tversion)
			}
		}
		ss.Frozen = false
		ss.LastNum = args.Num
		return shardrpc.InstallShardReply{Err: rpc.OK}

	case OpDeleteShard:
		args := op.Req.(*shardrpc.DeleteShardArgs)
		shid := args.Shard
		ss, ok := kv.Shards[shid]
		if ok {
			// only delete if num matches/greater or equal
			if args.Num >= ss.LastNum {
				delete(kv.Shards, shid)
			}
		}
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	// unknown op
	return nil
}

// Snapshot/Restore using labgob
type snapshotState struct {
	Shards     map[shardcfg.Tshid]*ShardState
	ClientLast map[int64]int64
	ClientRes  map[int64]any
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	kv.initStateIfNil()

	s := snapshotState{
		Shards:     make(map[shardcfg.Tshid]*ShardState),
		ClientLast: make(map[int64]int64),
		ClientRes:  make(map[int64]any),
	}
	for k, v := range kv.Shards {
		copyData := make(map[string]string)
		for kk, vv := range v.Data {
			copyData[kk] = vv
		}
		s.Shards[k] = &ShardState{
			Data:    copyData,
			Frozen:  v.Frozen,
			LastNum: v.LastNum,
		}
	}
	for k, v := range kv.ClientLast {
		s.ClientLast[k] = v
	}
	for k, v := range kv.ClientRes {
		s.ClientRes[k] = v
	}

	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	enc.Encode(s)
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	var s snapshotState
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	if dec.Decode(&s) != nil {
		// decode error: ignore
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Shards = s.Shards
	kv.ClientLast = s.ClientLast
	kv.ClientRes = s.ClientRes
}

// RPC handlers: they should wrap request into Op and call rsm.Submit.
// Note: rsm.Submit should return ErrWrongLeader if current peer is not leader.
// We expect rsm.Submit implemented in your rsm package.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	log.Printf("Server %d: Received Get(%s)", kv.me, args.Key)

	op := Op{Type: OpGet, Req: args}
	err, res := kv.rsm.Submit(op)
	if err == rpc.ErrWrongLeader {
		log.Printf("Server %d: Get(%s) failed - not leader", kv.me, args.Key)
		reply.Err = rpc.ErrWrongLeader
		return
	}
	gr := res.(rpc.GetReply)
	*reply = gr
	log.Printf("Server %d: Get(%s) succeeded", kv.me, args.Key)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	op := Op{
		Type:    OpPut,
		Req:     args,
		ClerkID: args.ClientId,
		ReqID:   args.ReqId,
	}
	err, res := kv.rsm.Submit(op)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	pr := res.(rpc.PutReply)
	*reply = pr
}

// FreezeShard / InstallShard / DeleteShard handlers: wrap as Op and Submit
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	op := Op{Type: OpFreezeShard, Req: args}
	err, res := kv.rsm.Submit(op)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	fr := res.(shardrpc.FreezeShardReply)
	*reply = fr
}

func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	op := Op{Type: OpInstallShard, Req: args}
	err, res := kv.rsm.Submit(op)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	ir := res.(shardrpc.InstallShardReply)
	*reply = ir
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	op := Op{Type: OpDeleteShard, Req: args}
	err, res := kv.rsm.Submit(op)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	dr := res.(shardrpc.DeleteShardReply)
	*reply = dr
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// initialize local maps to avoid nil access while rsm may call DoOp
	kv.initStateIfNil()

	return []tester.IService{kv, kv.rsm.Raft()}
}
