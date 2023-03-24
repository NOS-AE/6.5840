package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MRWorker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (worker *MRWorker) log(format string, v ...interface{}) {
	log.Printf("[Worker %d] %s", worker.id, fmt.Sprintf(format, v...))
}

func (worker *MRWorker) call() bool {
	args := RequestWorkArgs{
		WorkerId: worker.id,
	}
	reply := RequestWorkReply{}
	if !call("Coordinator.RequestWork", &args, &reply) {
		return false
	}
	if reply.IsMap {
		worker.handleMapWork(&reply)
	} else {
		worker.handleReduceWork(&reply)
	}
	return true
}

// all errors are ignored and logged, to trigger the timeout then soon the coordinator will retry it
// note: we can send a rpc to invoke a quick-fail
func (worker *MRWorker) handleMapWork(task *RequestWorkReply) {
	file, err := os.Open(task.Filename)
	if err != nil {
		worker.log("cannot open %v", task.Filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		worker.log("cannot read %v", content)
		return
	}
	file.Close()
	kva := worker.mapf(task.Filename, string(content))

	buckets := make([][]KeyValue, task.NReduce)
	for _, v := range kva {
		i := ihash(v.Key) % task.NReduce
		buckets[i] = append(buckets[i], v)
	}
	intermediateFiles := make([]string, task.NReduce)
	for i, b := range buckets {
		imFn := intermediateFn(worker.id, i)
		imFile, err := os.Create(imFn)
		if err != nil {
			worker.log("cannot create %v", imFn)
			return
		}
		enc := json.NewEncoder(imFile)
		if err = enc.Encode(b); err != nil {
			worker.log("cannot encode into %v", imFn)
			return
		}
		imFile.Close()
		intermediateFiles[i] = imFn
	}
	submitArgs := SubmitWorkArgs{
		TaskId:            task.TaskId,
		IntermediateFiles: intermediateFiles,
	}
	submitReply := SubmitWorkReply{} // ignore reply
	call("Coordinator.SubmitWork", submitArgs, &submitReply)
}

func intermediateFn(mapTaskId int, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

func (worker *MRWorker) handleReduceWork(work *RequestWorkReply) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := MRWorker{
		id:      os.Getpid(),
		mapf:    mapf,
		reducef: reducef,
	}
	for worker.call() {
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
