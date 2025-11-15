package mr

import (
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
}

type State struct {
	State    int
	Time     time.Time
	Filename string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Test(args *DummyArgs, reply *DummyReply) error {
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

	for _, status := range c.Reduce_array {
		if status.State != 2 {
			return false
		}
	}

	return true
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

	for _, filename := range files {
		c.Maps_array = append(c.Maps_array, State{
			State:    0,
			Time:     time.Now(),
			Filename: filename,
		})
	}

	c.server()
	return &c
}
