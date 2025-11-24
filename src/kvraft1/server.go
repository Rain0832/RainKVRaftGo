package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type ClientPutResult struct {
	ReqId  int64 // Unique ID for the client
	Result rpc.PutReply
}

type KVServer struct {
	me               int
	dead             int32 // set by Kill()
	rsm              *rsm.RSM
	mu               sync.RWMutex
	kvmap            map[string]ValueVersion
	clientPutResults map[int64]ClientPutResult // clientId -> ClientPutReq
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case *rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if prev, ok := kv.clientPutResults[args.ClientId]; ok {
			if args.ReqId <= prev.ReqId {
				kv.clientPutResults[args.ClientId] = ClientPutResult{
					ReqId:  args.ReqId,
					Result: prev.Result,
				}
				return prev.Result
			}
		}

		val, ok := kv.kvmap[args.Key]
		if !ok {
			if args.Version == 0 {
				// Key does not exist and version is 0, install the value
				kv.kvmap[args.Key] = ValueVersion{Value: args.Value, Version: 1}
				res := rpc.PutReply{Err: rpc.OK}
				kv.clientPutResults[args.ClientId] = ClientPutResult{
					ReqId:  args.ReqId,
					Result: res,
				}
				return res
			} else {
				// Key does not exist and version is not 0, return ErrNoKey
				res := rpc.PutReply{Err: rpc.ErrNoKey}
				kv.clientPutResults[args.ClientId] = ClientPutResult{
					ReqId:  args.ReqId,
					Result: res,
				}
				return res
			}
		}

		if args.Version != val.Version {
			// Version does not match, return ErrVersion
			res := rpc.PutReply{Err: rpc.ErrVersion}
			kv.clientPutResults[args.ClientId] = ClientPutResult{
				ReqId:  args.ReqId,
				Result: res,
			}
			return res
		}
		// Version matches, update the value and increment version
		kv.kvmap[args.Key] = ValueVersion{Value: args.Value, Version: val.Version + 1}

		res := rpc.PutReply{Err: rpc.OK}
		kv.clientPutResults[args.ClientId] = ClientPutResult{
			ReqId:  args.ReqId,
			Result: res,
		}
		return res

	case rpc.PutArgs:
		return kv.DoOp(&args)

	case *rpc.GetArgs:
		kv.mu.RLock()
		defer kv.mu.RUnlock()

		val, ok := kv.kvmap[args.Key]
		if !ok {
			// Key does not exist, return ErrNoKey
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		// Key exists, return the value and version
		return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
	case rpc.GetArgs:
		return kv.DoOp(&args)

	default:
		// Unsupported operation, return an error or nil
		log.Printf("KVServer[%d] received unsupported operation type: %T", kv.me, args)
		panic("unsupported operation type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	tmp := make(map[string]ValueVersion, len(kv.kvmap))
	for k, v := range kv.kvmap {
		tmp[k] = v
	}
	kv.mu.Unlock()

	buff := new(bytes.Buffer)
	ecoder := labgob.NewEncoder(buff)

	err := ecoder.Encode(tmp)
	if err != nil {
		log.Printf("KVServer[%d] snapshot encode error: %v", kv.me, err)
		return nil
	}

	return buff.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(data) == 0 {
		kv.kvmap = make(map[string]ValueVersion)
		return
	}

	buff := bytes.NewBuffer(data)
	dcoder := labgob.NewDecoder(buff)

	var snapshot map[string]ValueVersion
	err := dcoder.Decode(&snapshot)
	if err != nil {
		log.Printf("KVServer[%d] restore decode error: %v", kv.me, err)
		kv.kvmap = make(map[string]ValueVersion)
		return
	}

	kv.kvmap = snapshot
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	_, rep := kv.rsm.Submit(args)
	if rep == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(rpc.PutReply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(ClientPutResult{})
	labgob.Register(map[int64]ClientPutResult{})

	kv := &KVServer{
		me:               me,
		kvmap:            make(map[string]ValueVersion),
		clientPutResults: make(map[int64]ClientPutResult),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
