package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
		args := RequestTaskReply{}
		reply := RequestTaskReply{}

		res := call("Coordinator.RequestTask", &args, &reply)
		if !res {
			break
		}

		switch reply.Task.TaskType {
		case MAP:
			doMap(&reply, mapf)
		case REDUCE:
			doReduce(&reply, reducef)
		case WAIT:
			time.Sleep(1 * time.Second)
		case EXIT:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
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

func doMap(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.Task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.Index, r)
		ofile, _ := ioutil.TempFile(".", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	reply.Task.Status = FINISH
	replyEx := RequestTaskReply{}
	call("Coordinator.NotifyComplete", &reply, &replyEx)
}

func doReduce(reply *RequestTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, filename := range reply.Task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", reply.Task.Index)
	ofile, _ := ioutil.TempFile("", oname)

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

	ofile.Close()
	os.Rename(ofile.Name(), oname)

	reply.Task.Status = FINISH
	replyEx := RequestTaskReply{}
	call("Coordinator.NotifyComplete", &reply, &replyEx)
}
