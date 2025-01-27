package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

// Each Task should have starttime, id, filename(to be reproduced), and its state
type Task struct {
	ID        int
	State     TaskState
	StartTime time.Time
	FileName  string
}

type TaskState int

const (
	Started TaskState = iota
	Completed
)

type Coordinator struct {
	// Your definitions here.
	files []string
	// State of tasks
	Tasks   []Task
	phase   string
	nMap    int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequestArgs, response *TaskResponse) error {
	// Add lock
	if c.phase == "Map" {
		for i := 0; i < c.nMap; i++ {
			task := c.Tasks[i]
			if task.StartTime.IsZero() || (time.Now().Sub(task.StartTime).Seconds() >= 10 && task.State != Completed) {
				task.State = Started
				task.StartTime = time.Now()
				response.Type = Map
				response.FileName = task.FileName
				response.NumReduce = c.nReduce
				response.TaskID = i
			}
		}
		return nil
	} else if c.phase == "Reduce" {
		for i := c.nMap; i < c.nMap+c.nReduce; i++ {
			task := c.Tasks[i]
			if task.StartTime.IsZero() || (time.Now().Sub(task.StartTime).Seconds() >= 10 && task.State != Completed) {
				task.State = Started
				task.StartTime = time.Now()
				response.Type = Reduce
				response.NumReduce = c.nReduce
				response.TaskID = i
			}
		}
		return nil
	}
	return nil
}

func (c *Coordinator) MarkAsComplete(response *TaskResponse) {
	if response.Type == Complete {
		c.Tasks[response.TaskID].State = Completed
	} else {
		log.Fatalf("There is a contradict about task %v", c.Tasks[response.TaskID])
	}
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
	for _, task := range c.Tasks {
		if task.State != Completed {
			return ret
		}
	}
	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		phase:   "Map",
		Tasks:   make([]Task, len(files)+nReduce),
		nMap:    len(files),
		nReduce: nReduce,
	}

	// Your code here.
	for i, file := range files {
		c.Tasks[i] = Task{
			ID:       i,
			FileName: file,
		}
	}

	for i := len(files); i < len(files)+nReduce; i++ {
		c.Tasks[i] = Task{
			ID: i,
		}
	}

	c.server()
	return &c
}
