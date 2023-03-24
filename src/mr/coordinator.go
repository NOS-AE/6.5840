package mr

import (
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	nMap         int
	mu           sync.Mutex
	tasks        chan *mrtask
	taskNum      int
	nextTaskId   int
	runningTasks map[int]*mrtask // key: taskid, value: task
}

type mrtask struct {
	isMap     bool
	taskId    int // taskId is auto-incremented
	filename  string
	taskIndex int
	timer     *time.Timer
}

var doneErr = errors.New("done")

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	task, ok := <-c.tasks
	if !ok {
		return doneErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.runningTasks[task.taskId] = task
	c.setTimeout(task)

	if task.isMap {
		reply.IsMap = true
		reply.TaskId = task.taskId
		reply.Filename = task.filename
		reply.NReduce = c.nReduce
		reply.TaskIndex = task.taskIndex
	} else {
		reply.IsMap = false
		reply.TaskId = task.taskId
		reply.NMap = c.nMap
		reply.TaskIndex = task.taskIndex
	}
	return nil
}

const taskTimeout = 10 * time.Second

func (c *Coordinator) setTimeout(task *mrtask) {
	task.timer = time.AfterFunc(taskTimeout, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.runningTasks[task.taskId]; ok {
			delete(c.runningTasks, task.taskId)
			task.taskId = c.genTaskId()
			c.tasks <- task
		}
	})
}

var timeoutErr = errors.New("timeout")

func (c *Coordinator) SubmitWork(args *SubmitWorkArgs, reply *SubmitWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, ok := c.runningTasks[args.TaskId]
	// if not ok, we can know that the task is timeout and deleted by timer
	if !ok {
		return timeoutErr
	}
	task.timer.Stop()
	delete(c.runningTasks, task.taskId)

	// since a work is submitted, update the coordinator
	c.taskNum--
	if c.taskNum == 0 {
		close(c.tasks)
	} else if c.taskNum == c.nReduce {
		for i := 0; i < c.nReduce; i++ {
			task = &mrtask{
				isMap:     false,
				taskId:    c.genTaskId(),
				taskIndex: i,
			}
			c.tasks <- task
		}
	}

	return nil
}

// non-thread-safe
func (c *Coordinator) genTaskId() int {
	c.nextTaskId++
	return c.nextTaskId
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.taskNum == 0
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		tasks:   make(chan *mrtask, int(math.Max(float64(nReduce), float64(len(files))))),
		taskNum: len(files) + nReduce,
		// taskNum:      len(files),
		runningTasks: make(map[int]*mrtask),
		nextTaskId:   1,
	}

	for i, f := range files {
		c.tasks <- &mrtask{
			isMap:     true,
			taskId:    c.genTaskId(),
			filename:  f,
			taskIndex: i,
		}
	}

	c.server()
	return &c
}
