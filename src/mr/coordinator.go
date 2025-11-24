package mr

//
// coordinator.go
//

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus struct {
	Assigned  bool
	Completed bool
	StartTime time.Time
}

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	nReduce int

	mapTasks    []TaskStatus
	reduceTasks []TaskStatus
	stage       string // "MAP" or "REDUCE" or "DONE"
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *ReplyTaskArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.stage {
	case "MAP":
		for i, f := range c.files {
			t := &c.mapTasks[i]
			if !t.Assigned || (t.Assigned && !t.Completed && time.Since(t.StartTime) > 10*time.Second) {
				reply.TaskType = "MAP"
				reply.FileName = f
				reply.MapID = i
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				t.Assigned = true
				t.StartTime = time.Now()
				log.Printf("Coordinator: assigned MAP %d -> %s\n", i, f)
				return nil
			}
		}
		if allCompleted(c.mapTasks) {
			c.stage = "REDUCE"
			log.Println("Coordinator: All MAP done. Switch to REDUCE stage.")
			reply.TaskType = "WAIT"
		} else {
			reply.TaskType = "WAIT"
		}

	case "REDUCE":
		for i := 0; i < c.nReduce; i++ {
			t := &c.reduceTasks[i]
			if !t.Assigned || (t.Assigned && !t.Completed && time.Since(t.StartTime) > 10*time.Second) {
				reply.TaskType = "REDUCE"
				reply.ReduceID = i
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				t.Assigned = true
				t.StartTime = time.Now()
				log.Printf("Coordinator: assigned REDUCE %d\n", i)
				return nil
			}
		}
		if allCompleted(c.reduceTasks) {
			c.stage = "DONE"
			reply.TaskType = "EXIT"
			log.Println("Coordinator: All REDUCE done. Workers can exit.")
		} else {
			reply.TaskType = "WAIT"
		}

	case "DONE":
		reply.TaskType = "EXIT"
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReplyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "MAP" {
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			c.mapTasks[args.TaskID].Completed = true
			log.Printf("Coordinator: MAP %d done\n", args.TaskID)
		}
	} else if args.TaskType == "REDUCE" {
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			c.reduceTasks[args.TaskID].Completed = true
			log.Printf("Coordinator: REDUCE %d done\n", args.TaskID)
		}
	}

	reply.OK = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Coordinator: RPC server listening on %v\n", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stage == "DONE"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReduce),
		stage:       "MAP",
	}

	c.server()

	// Check the
	go func() {
		for {
			time.Sleep(time.Second)
			c.mu.Lock()
			if c.stage == "MAP" && allCompleted(c.mapTasks) {
				c.stage = "REDUCE"
				log.Println("Coordinator: All MAP done. Switch to REDUCE stage.")
			}
			if c.stage == "REDUCE" && allCompleted(c.reduceTasks) {
				c.stage = "DONE"
				log.Println("Coordinator: All REDUCE done. Workers can exit.")
			}
			c.mu.Unlock()
		}
	}()

	return &c
}

func allCompleted(tasks []TaskStatus) bool {
	for _, t := range tasks {
		if !t.Completed {
			return false
		}
	}
	return true
}
