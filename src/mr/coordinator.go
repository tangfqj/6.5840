package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	InputFiles []string
	NReduce    int

	MapTasks    []MRTask
	ReduceTasks []MRTask

	MapAssigned    int
	ReduceAssigned int

	AllMapDone    bool
	AllReduceDone bool

	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) NotifyComplete(args *RequestTaskReply, reply *RequestTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.Task.TaskType == MAP {
		c.MapTasks[args.TaskNo] = args.Task
	} else if args.Task.TaskType == REDUCE {
		c.ReduceTasks[args.TaskNo] = args.Task
	}

	return nil
}

func (c *Coordinator) RequestTask(args, reply *RequestTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// If there is still unassigned task
	if c.MapAssigned < len(c.MapTasks) {
		reply.TaskNo = c.MapAssigned
		c.MapTasks[c.MapAssigned].Timestamp = time.Now()
		reply.Task = c.MapTasks[c.MapAssigned]
		reply.Task.Status = ASSIGN
		reply.NReduce = c.NReduce

		c.MapAssigned++
		return nil
	}

	// All assigned
	// We have to ensure that all map tasks are completed
	if !c.AllMapDone {
		for index, mapTask := range c.MapTasks {
			if mapTask.Status != FINISH {
				if time.Since(mapTask.Timestamp) > 10*time.Second {
					reply.TaskNo = index
					c.MapTasks[index].Timestamp = time.Now()
					reply.Task = c.MapTasks[index]
					reply.Task.Status = ASSIGN
					reply.NReduce = c.NReduce
					return nil
				} else {
					reply.Task.TaskType = WAIT
					return nil
				}
			}
		}
		c.AllMapDone = true
	}

	// All map completed, then we can assign reduce tasks
	if c.ReduceAssigned < len(c.ReduceTasks) {
		reply.TaskNo = c.ReduceAssigned
		c.ReduceTasks[c.ReduceAssigned].Timestamp = time.Now()
		reply.Task = c.ReduceTasks[c.ReduceAssigned]
		reply.Task.Status = ASSIGN
		reply.NReduce = c.NReduce
		c.ReduceAssigned++
		return nil
	}

	// All reduce tasks assigned, check wheter they are completed
	if !c.AllReduceDone {
		for index, reduceTask := range c.ReduceTasks {
			if reduceTask.Status != FINISH {
				if time.Since(reduceTask.Timestamp) > 10*time.Second {
					reply.TaskNo = index
					c.ReduceTasks[index].Timestamp = time.Now()
					reply.Task = c.ReduceTasks[index]
					reply.Task.Status = ASSIGN
					reply.NReduce = c.NReduce
					return nil
				} else {
					reply.Task.TaskType = WAIT
					return nil
				}
			}
		}
		c.AllReduceDone = true
	}

	reply.Task.Status = Status(EXIT)
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	return c.AllMapDone && c.AllReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:     files,
		NReduce:        nReduce,
		MapTasks:       make([]MRTask, len(files)),
		ReduceTasks:    make([]MRTask, nReduce),
		MapAssigned:    0,
		ReduceAssigned: 0,
		AllMapDone:     false,
		AllReduceDone:  false,
		Mutex:          sync.Mutex{},
	}

	// Initialize mapTasks
	for i := range c.MapTasks {
		c.MapTasks[i] = MRTask{
			TaskType:    MAP,
			Status:      UNASSIGN,
			Timestamp:   time.Now(),
			Index:       i,
			InputFiles:  []string{files[i]},
			OutputFiles: nil,
		}
	}

	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = MRTask{
			TaskType:    REDUCE,
			Status:      UNASSIGN,
			Timestamp:   time.Now(),
			Index:       i,
			InputFiles:  generateInputFiles(i, len(files)),
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}

	c.server()
	return &c
}

func generateInputFiles(i, map_cnt int) (res []string) {
	for j := 0; j < map_cnt; j++ {
		res = append(res, fmt.Sprintf("mr-%d-%d", j, i))
	}
	return
}
