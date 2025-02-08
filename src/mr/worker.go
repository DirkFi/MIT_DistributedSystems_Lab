package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Worker needs to respond to Coordinator to process Map or Reduce Task
	// For coordinator, if it gets Complete, it should assign a new task to that worker?
	// nTasks = nFiles
	for {
		taskArgs := TaskCompleteArgs{}
		taskResponse := TaskResponse{}

		ok := call("Coordinator.AssignTask", &taskArgs, &taskResponse)

		if !ok {
			log.Println("Failure to contact the coordinator. Exit the worker.")
			return
		}

		switch taskResponse.Type {
		case Map:
			doMapTask(mapf, &taskResponse)
		case Reduce:
			doReduceTask(reducef, &taskResponse)
		case Wait:
			fmt.Println("Waiting..")
			time.Sleep(1 * time.Millisecond)
		case Complete:
			return
		default:
			panic(fmt.Sprintf("Unexpected JobType %v", taskResponse.Type))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, taskResponse *TaskResponse) {
	// The input of mapf is (filename, content)
	// After it is done need to change the state of tasks (use a function writtien in coordinator.go)

	// Save (hash, []kvs)
	intermediate := []KeyValue{}

	// same structure form mrsequential.go
	filename := taskResponse.FileName
	nReduce := taskResponse.NumReduce

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("response in doMapTask", taskResponse.FileName)
		log.Fatalf("Cannot open %v in worker while doing map tasks", filename)
	}
	content, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("Cannot read %v in worker after opening files", filename)
	}

	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	hashedValues := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		hash := ihash(kv.Key) % nReduce
		hashedValues[hash] = append(hashedValues[hash], kv)
	}
	// Write into files
	for rNum, kvs := range hashedValues {
		fileName := fmt.Sprintf("mr-%d-%d", taskResponse.TaskID, rNum)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("Cannot create %v in worker after hashedValues are done", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("Cannot write %v into the %v", &kv, fileName)
			}
		}
		file.Close()
	}

	// Call the function to mark TaskID as Completed
	completeArgs := TaskCompleteArgs{
		TaskID: taskResponse.TaskID,
		Type:   Complete,
	}
	taskResponse.Type = Complete
	if !call("Coordinator.MarkAsComplete", &completeArgs, &taskResponse) {
		log.Println("Failed to notify coordinator about task completion")
	}
	fmt.Println("Current file name(worker) is", taskResponse.FileName)
}

func doReduceTask(reducef func(string, []string) string, taskResponse *TaskResponse) {
	// reducef(intermediate[i].Key, values)
	// The input of reducef is (key, values)
	intermediate := []KeyValue{}
	filename := taskResponse.FileName

	// Open the intermediate file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open intermediate file %v: %v", filename, err)
	}
	defer file.Close()

	// Decode the key-value pairs from the file
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break // End of file
		}
		intermediate = append(intermediate, kv)
	}

	groups := make(map[string][]string)
	for _, kv := range intermediate {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	outputFile := fmt.Sprintf("mr-out-%d", taskResponse.TaskID-taskResponse.NumMap)
	ofile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Cannot create output file %v: %v", outputFile, err)
	}
	defer ofile.Close()

	for key, values := range groups {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	completeArgs := TaskCompleteArgs{
		TaskID: taskResponse.TaskID,
		Type:   Complete,
	}
	taskResponse.Type = Complete
	if !call("Coordinator.MarkAsComplete", &completeArgs, &taskResponse) {
		log.Println("Failed to notify coordinator about task completion")
	}
}

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
