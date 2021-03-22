package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "path/filepath"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := Args{}
		reply := Reply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}
		if reply.Job == "map" {
			// Open file, read it, and call the map function
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))

			// Write intermediate key/value pairs into intermediate files
			encoders := make([]*json.Encoder, reply.NReduce)
			for i := range encoders {
				filename := fmt.Sprintf("mr-intermediate-%d-%d", reply.TaskNum, i)
				file, err = os.Create(filename)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()
				enc := json.NewEncoder(file)
				encoders[i] = enc
			}
			for _, kv := range kva {
				idx := ihash(kv.Key) % reply.NReduce
				err := encoders[idx].Encode(kv)
				if err != nil {
					log.Fatal(err)
				}
			}
			args.Job = "map"
			args.TaskNum = reply.TaskNum
			call("Coordinator.TaskDone", &args, &reply)
		} else if reply.Job == "reduce" {
			pattern := fmt.Sprintf("mr-intermediate-*-%d", reply.TaskNum)
			files, err := filepath.Glob(pattern)
			if err != nil {
				log.Fatal(err)
			}
			intermediate := []KeyValue{}
			for _, filename := range files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.TaskNum)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

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

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			args.Job = "reduce"
			args.TaskNum = reply.TaskNum
			call("Coordinator.TaskDone", &args, &reply)
		} else if reply.Job == "wait" {
			time.Sleep(1 * time.Second)
		} else if reply.Job == "exit" {
			return
		} else {
			log.Fatalf("Invalid reply.Job: %v", reply.Job)
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
