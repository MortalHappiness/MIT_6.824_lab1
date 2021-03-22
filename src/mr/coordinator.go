package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const (
	AVAILABLE = iota
	PENDING   = iota
	FINISH    = iota
)

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	files          []string
	nReduce        int
	mapStatus      []int
	reduceStatus   []int
	mapFinished    bool
	reduceFinished bool
	mapTimers      []*time.Timer
	reduceTimers   []*time.Timer
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapFinished {
		taskNum := -1
		for idx, status := range c.mapStatus {
			if status == AVAILABLE {
				taskNum = idx
				break
			}
		}
		if taskNum == -1 {
			reply.Job = "wait"
		} else {
			reply.Job = "map"
			reply.Filename = c.files[taskNum]
			reply.TaskNum = taskNum
			reply.NReduce = c.nReduce
			c.mapStatus[taskNum] = PENDING
			c.mapTimers[taskNum] = time.AfterFunc(10*time.Second, func() {
				c.mu.Lock()
				c.mapStatus[taskNum] = AVAILABLE
				c.mu.Unlock()
			})
		}
	} else if !c.reduceFinished {
		taskNum := -1
		for idx, status := range c.reduceStatus {
			if status == AVAILABLE {
				taskNum = idx
				break
			}
		}
		if taskNum == -1 {
			reply.Job = "wait"
		} else {
			reply.Job = "reduce"
			reply.TaskNum = taskNum
			c.reduceStatus[taskNum] = PENDING
			c.reduceTimers[taskNum] = time.AfterFunc(10*time.Second, func() {
				c.mu.Lock()
				c.reduceStatus[taskNum] = AVAILABLE
				c.mu.Unlock()
			})
		}
	} else {
		reply.Job = "exit"
	}

	return nil
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	job := args.Job
	switch job {
	case "map":
		if c.mapStatus[args.TaskNum] == PENDING {
			c.mapTimers[args.TaskNum].Stop()
			c.mapStatus[args.TaskNum] = FINISH
		}
		finished := true
		for _, status := range c.mapStatus {
			if status != FINISH {
				finished = false
				break
			}
		}
		c.mapFinished = finished
	case "reduce":
		if c.reduceStatus[args.TaskNum] == PENDING {
			c.reduceTimers[args.TaskNum].Stop()
			c.reduceStatus[args.TaskNum] = FINISH
		}
		finished := true
		for _, status := range c.reduceStatus {
			if status != FINISH {
				finished = false
				break
			}
		}
		c.reduceFinished = finished
	default:
		log.Fatalf("Invalid args.Job: %v\n", args.Job)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapFinished && c.reduceFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.mapFinished = false
	c.reduceFinished = false
	c.mapStatus = make([]int, len(files))
	c.reduceStatus = make([]int, nReduce)
	for i := range files {
		c.mapStatus[i] = AVAILABLE
	}
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = AVAILABLE
	}
	c.mapTimers = make([]*time.Timer, len(files))
	c.reduceTimers = make([]*time.Timer, nReduce)

	c.server()
	return &c
}
