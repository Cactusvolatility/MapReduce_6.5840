package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	//CallCoordinator()

	args := CommArgs{}
	args.Instruction = "Available"
	reply := CommReply{}

	// right now just use Fatalf to exit the program if we encounter an error
	for {
		ok := call("Coordinator.CommHandle", &args, &reply)
		if ok {
			switch reply.CommRun {
			case "Map":
				fmt.Printf("received command to Map, run map on payload\n")
				file, err := os.Open(reply.Payload)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Payload)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("unable to read %v", reply.Payload)
				}
				file.Close()
				// mapf returns a list of KeyValue
				kva := mapf(reply.Payload, string(content))

				// TODO:
				// add .tmp file writing in case of crash

				// make a new encoder for each file that we're making
				// keep all files open
				encoders := make([]*json.Encoder, reply.NReduce)

				for x := 0; x < reply.NReduce; x++ {
					oname := fmt.Sprintf("mr-%v-%v", reply.Mapid, x)
					file, err := os.Create(oname)
					if err != nil {
						log.Fatalf("cannot create %v", oname)
					}
					encoders[x] = json.NewEncoder(file)
				}

				for _, kv := range kva {
					bucket := ihash(kv.Key) % reply.NReduce
					// directly add to this file
					encoders[bucket].Encode(&kv)
				}

				args.Instruction = "MapDone"
				args.Id = reply.Mapid

			case "Reduce":
				fmt.Printf("received command to Reduce, run reduce on payload\n")

				// find all files that fall into this bucket
				Pattern := fmt.Sprintf("mr-*-%d", reply.Reduceid)
				matches, err := filepath.Glob(Pattern)
				fmt.Printf("There are %d files that match this Reduceid %d \n", len(matches), reply.Reduceid)
				if err != nil {
					log.Fatalf("No files matching this Reduce Partition %d!", reply.Reduceid)
				}
				// for each file
				// this is inefficient but that's the point. We make it back in parallelism

				intermediate := []KeyValue{}
				for _, filename := range matches {

					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}

					// from hints - we decode and add it back
					dec := json.NewDecoder(file)
					for {
						// ???
						// oh this makes an empty value
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}

					file.Close()
				}

				sort.Sort(ByKey(intermediate))

				oname := fmt.Sprintf("mr-out-%d", reply.Reduceid)
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
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

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				ofile.Close()
				args.Instruction = "ReduceDone"
				args.Id = reply.Reduceid

			case "Complete":
				fmt.Printf("Worker received shutdown signal\n")
				return

			default:
				fmt.Printf("no work available, waiting rn\n")
				// wait certain amount of time
				// 5 seconds?
				time.Sleep(2 * time.Second)
			}

		} else {
			fmt.Printf("call failed!\n")
			return
		}
	}
}

// CallCoordinator never has access to mapf and reducef - keep it inside of Worker
/*
func CallCoordinator() {

	isComplete := false
	args := CommArgs{}
	args.Instruction = "Available"
	reply := CommReply{}

	for !isComplete {
		ok := call("Coordinator.CommHandle", &args, &reply)
		if ok {
			switch reply.CommRun {
			case "Map":
				fmt.Printf("received command to Map, run map on payload\n")
				// implement Map

			case "Reduce":
				fmt.Printf("received command to Reduce, run reduce on payload\n")
				// implement Reduce
				// use ihash here

			case "Complete":
				fmt.Printf("Worker received shutdown signal\n")
				isComplete = true

			default:
				fmt.Printf("no work available, waiting rn\n")
				// wait certain amount of time
				// 5 seconds?
				time.Sleep(5 * time.Second)
			}

		} else {
			fmt.Printf("call failed!\n")
			break
		}
	}
}*/

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
