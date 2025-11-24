package mr

//
// worker.go
//

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := RequestTask()

		switch reply.TaskType {
		case "MAP":
			fmt.Printf("Worker: received MAP task %d, file=%s\n", reply.MapID, reply.FileName)
			doMapTask(reply, mapf)
			ReportTaskCompleted("MAP", reply.MapID)

		case "REDUCE":
			fmt.Printf("Worker: received REDUCE task %d\n", reply.ReduceID)
			doReduceTask(reply, reducef)
			ReportTaskCompleted("REDUCE", reply.ReduceID)

		case "WAIT":
			time.Sleep(500 * time.Millisecond)

		case "EXIT":
			fmt.Println("Worker: exiting.")
			return

		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func RequestTask() ReplyTaskArgs {
	args := RequestTaskArgs{}
	reply := ReplyTaskArgs{}

	backoff := 100 * time.Millisecond
	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			return reply
		}
		log.Printf("Worker: cannot reach coordinator, will retry in %v...\n", backoff)
		time.Sleep(backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func ReportTaskCompleted(taskType string, taskID int) {
	args := ReportTaskArgs{TaskType: taskType, TaskID: taskID}
	var reply ReportTaskReplyArgs

	backoff := 100 * time.Millisecond
	for tries := 0; tries < 8; tries++ {
		ok := call("Coordinator.ReportTask", &args, &reply)
		if ok {
			if reply.OK {
				fmt.Printf("Worker: reported completion of %s %d\n", taskType, taskID)
			}
			return
		}
		log.Printf("Worker: failed to report task completion for %s %d, retrying in %v...\n", taskType, taskID, backoff)
		time.Sleep(backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	log.Printf("Worker: giving up reporting competition for %s %d after retries", taskType, taskID)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("call: dialing error: %v (rpcname=%s)\n", err, rpcname)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("call: RPC %s error: %v\n", rpcname, err)
	return false
}

func doMapTask(reply ReplyTaskArgs, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	nReduce := reply.NReduce

	// prepare mid file for each reduce task
	encoders := make([]*json.Encoder, nReduce)
	tmpFiles := make([]*os.File, nReduce)
	tmpNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", reply.MapID, i))
		if err != nil {
			log.Fatalf("doMapTask: cannot create temp file: %v", err)
		}
		tmpFiles[i] = tmp
		tmpNames[i] = tmp.Name()
		encoders[i] = json.NewEncoder(tmp)
	}

	// write each KeyValue to its corresponding area
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		if err := encoders[r].Encode(&kv); err != nil {
			log.Fatalf("doMapTask: encode error: %v", err)
		}
	}

	for i := 0; i < nReduce; i++ {
		tmpFiles[i].Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.MapID, i)
		if err := os.Rename(tmpNames[i], finalName); err != nil {
			log.Fatalf("doMapTask: rename %v -> %v failed: %v", tmpNames[i], finalName, err)
		}
	}
}

func doReduceTask(reply ReplyTaskArgs, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	// read all mr-X-reduceID intermediate files
	for m := 0; m < reply.NMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reply.ReduceID)
		file, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("doReduceTask: %v does not exist, skipping\n", filename)
				continue
			}
			log.Printf("doReduceTask: cannot open %v: %v (skipping)\n", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort intermediate by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// create output file
	tmpOut, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", reply.ReduceID))
	if err != nil {
		log.Fatalf("doReduceTask: cannot create temp file: %v", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpOut, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpOut.Close()
	finalOut := fmt.Sprintf("mr-out-%d", reply.ReduceID)
	if err := os.Rename(tmpOut.Name(), finalOut); err != nil {
		log.Fatalf("doReduceTask: rename %v -> %v failed: %v", tmpOut.Name(), finalOut, err)
	}
}
