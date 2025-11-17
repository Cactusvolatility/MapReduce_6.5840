package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// see:
//
//	https://go.dev/src/net/rpc/server.go
type Coordinator struct {
	// Your definitions here.
	Completed bool
	//Workers int
	Maps_array   []State
	Reduce_array []State
	RespLock     sync.Mutex
	NReduce      int
	MComplete    bool
	RComplete    bool
}

type State struct {
	State    int
	Time     time.Time
	Filename string
}

func (c *Coordinator) allMapsDone() bool {
	for _, t := range c.Maps_array {
		if t.State != 2 { // 2 = Done
			return false
		}
	}
	return true
}

func (c *Coordinator) allReducesDone() bool {
	for _, t := range c.Reduce_array {
		if t.State != 2 {
			//fmt.Printf("Reduce is done \n")
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CommHandle(args *CommArgs, reply *CommReply) error {

	// implement case here
	// when Map is done then we go to Reduce
	// use the lock to let prevent races
	c.RespLock.Lock()
	defer c.RespLock.Unlock()

	// worker reply
	if args.Instruction == "MapDone" {
		c.Maps_array[args.Id].State = 2
	}

	if args.Instruction == "ReduceDone" {
		c.Reduce_array[args.Id].State = 2
		fmt.Printf("I completed reduce on %v \n", args.Id)
	}

	c.MComplete = c.allMapsDone()
	c.RComplete = c.allReducesDone()

	// if done
	if c.MComplete && c.RComplete {
		fmt.Printf("both are complete, Mcomplete is %v, Rcomplete is %v \n", c.MComplete, c.RComplete)
		reply.CommRun = "Complete"
		c.Completed = true
		return nil
	}

	// if map not complete
	if !c.MComplete {
		for i := range c.Maps_array {
			t := &c.Maps_array[i]

			if t.State == 0 {
				t.State = 1
				t.Time = time.Now()
				reply.CommRun = "Map"
				reply.Mapid = i
				reply.NReduce = c.NReduce
				reply.Payload = t.Filename
				return nil
			}

			if t.State == 1 && time.Since(t.Time) > 10*time.Second {
				t.Time = time.Now()
				reply.CommRun = "Map"
				reply.Mapid = i
				reply.NReduce = c.NReduce
				reply.Payload = t.Filename
				return nil
			}
		}

		reply.CommRun = "Wait"
		return nil
	}

	// if reduce not complete
	if !c.RComplete {
		for i := range c.Reduce_array {
			t := &c.Reduce_array[i]

			if t.State == 0 {
				t.State = 1
				t.Time = time.Now()
				reply.CommRun = "Reduce"
				reply.Reduceid = i
				reply.NReduce = c.NReduce
				return nil
			}

			if t.State == 1 && time.Since(t.Time) > 10*time.Second {
				t.Time = time.Now()
				reply.CommRun = "Reduce"
				reply.Reduceid = i
				reply.NReduce = c.NReduce
				return nil
			}
		}

		reply.CommRun = "Wait"
		return nil
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// check if states of the reduce are complete
	if c.RComplete && c.MComplete {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Completed = false
	c.Maps_array = []State{}
	c.Reduce_array = []State{}
	c.NReduce = nReduce
	c.MComplete = false
	c.RComplete = false

	for _, filename := range files {
		c.Maps_array = append(c.Maps_array, State{
			State:    0,
			Time:     time.Now(),
			Filename: filename,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.Reduce_array = append(c.Reduce_array, State{
			State:    0,
			Time:     time.Now(),
			Filename: "",
		})
	}

	fmt.Printf("nReduce is %d, length is %d", nReduce, len(c.Maps_array))
	c.server()
	return &c
}
