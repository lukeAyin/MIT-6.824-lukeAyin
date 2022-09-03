package mr

import (
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapWaiting    SyncedQueue
	MapRunning    SyncedSet
	ReduceWaiting SyncedQueue
	ReduceRunning SyncedSet

	mapTask       SyncedSet
	reduceTask    SyncedSet
	NReduce       int
	InFileList    []string
	interFileList [][]string
	OutFilePrefix string
	outTime       time.Duration
	MapMutex      sync.Mutex
	ReduceMutex   sync.Mutex
	mapDone       bool
	allDone       bool
}

func (c *Coordinator) MapDone() bool {
	return c.mapDone
}

func (c *Coordinator) AllDone() bool {
	return c.allDone
}

func (c *Coordinator) init(input []string, reduce int) {
	c.NReduce = reduce
	c.InFileList = input
	c.mapDone = false
	c.allDone = false
	c.OutFilePrefix = "mr-out-"
	c.MapWaiting.Init()
	c.ReduceWaiting.Init()
	c.MapRunning.Init()
	c.ReduceRunning.Init()
	c.mapTask.Init()
	c.reduceTask.Init()
	c.interFileList = make([][]string, reduce)
	for i := 0; i < len(c.interFileList); i++ {
		c.interFileList[i] = make([]string, 0)
	}
	c.generateMapTaskQueue()
}

func (c *Coordinator) generateMapTaskQueue() {
	log.Printf("init map task queue\n")
	for i := 0; i < len(c.InFileList); i++ {
		task := Task{
			taskId: i, outTime: c.outTime, nReduce: c.NReduce,
			beginTime: time.Now(), taskType: TaskMap, inputFileList: make([]string, 1),
		}
		task.inputFileList[0] = c.InFileList[i]
		c.MapWaiting.push(task)
		c.mapTask.insert(task)
	}
	log.Printf("%v map task created Done ", c.MapWaiting.Size())
}

func (c *Coordinator) generateReduceTaskQueue() {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	for i := 0; i < c.NReduce; i++ {
		task := Task{
			taskId:        1000 + i,
			outTime:       c.outTime,
			nReduce:       c.NReduce,
			beginTime:     time.Now(),
			inputFileList: c.interFileList[i],
			taskType:      TaskReduce,
		}
		c.ReduceWaiting.push(task)
		c.reduceTask.insert(task)
	}
	log.Printf("%v reduce task created Done", c.ReduceWaiting.Size())
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) disMapTask(taskInfo *TaskInfo) error {
	c.MapMutex.Lock()
	defer c.MapMutex.Unlock()
	if c.MapWaiting.Size() != 0 {
		task, err := c.MapWaiting.Pop()
		if err != nil {
			log.Fatalf("map waiting queue pop error: %v", err)
		}
		err = c.MapRunning.insert(task)
		*taskInfo = task.GenerateTaskInfo()
		return nil
	} else {
		task, err := c.MapRunning.get()
		if err != nil {
			log.Fatalf("map running pop error: %v", err)
		}
		*taskInfo = task.GenerateTaskInfo()
		log.Printf("map task running left : %v ", c.MapRunning.Size())
		return nil
	}
}

func (c *Coordinator) disReduceTask(taskInfo *TaskInfo) {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	if c.ReduceWaiting.Size() != 0 {
		task, err := c.ReduceWaiting.Pop()
		if err != nil {
			log.Fatalf("reduce waiting queue pop error: %v", err)
		}
		err = c.ReduceRunning.insert(task)
		*taskInfo = task.GenerateTaskInfo()
	} else {
		task, err := c.ReduceRunning.get()
		if err != nil {
			log.Fatalf("reduce running pop error: %v", err)
		}
		log.Printf("map task running left : %v ", c.MapRunning)
		*taskInfo = task.GenerateTaskInfo()
	}
}

func (c *Coordinator) AskTask(args *ExampleArgs, taskInfo *TaskInfo) error {
	if !c.MapDone() {
		c.disMapTask(taskInfo)
	} else if !c.AllDone() {
		c.disReduceTask(taskInfo)
	}
	taskInfo.state = TaskEnd
	return nil
}
func (c *Coordinator) mapTaskDone(info *TaskInfo, reply *Reply) {
	c.MapMutex.Lock()
	defer c.MapMutex.Unlock()
	defer func() {
		for i := 0; i < len(reply.OutPutFileList); i++ {
			os.Remove(reply.OutPutFileList[i])
		}
	}()
	log.Printf("Map %v  task  file %v complete\n ", info.taskId, info.inputFileList)
	if !c.mapTask.find(info.taskId) {
		return
	}
	c.mapTask.remove(info.taskId)
	c.MapRunning.remove(info.taskId)
	for i := 0; i < len(c.interFileList); i++ {
		c.interFileList[i] = append(c.interFileList[i], reply.OutPutFileList[i])
	}
	if c.mapTask.Size() == 0 {
		c.mapDone = true
		c.generateReduceTaskQueue()
	}
}

func (c *Coordinator) reduceTaskDone(info *TaskInfo, reply *Reply) {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	log.Printf("reduce %v task file %v complete\n", info.taskId, info.inputFileList)
	defer os.Remove(reply.OutPutFileList[0])
	if !c.reduceTask.find(info.taskId) {
		return
	}
	c.ReduceRunning.remove(info.taskId)
	c.reduceTask.remove(info.taskId)
	os.Rename(filepath.Join(reply.OutPutFileList[0]), c.OutFilePrefix+strconv.Itoa(info.taskId))
}

func (c *Coordinator) TaskDone(taskInfo *TaskInfo, reply *Reply) error {
	//var taskType string
	//if taskInfo.state == TaskMap {
	//	taskType = "map task"
	//} else {
	//	taskType = "reduce task"
	//}
	//log.Printf("a worker finish its task! taskId:%v taskType:%v", taskInfo.taskId, taskType)
	switch taskInfo.state {
	case TaskMap:
		c.mapTaskDone(taskInfo, reply)
		break
	case TaskReduce:
		c.reduceTaskDone(taskInfo, reply)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	return c.AllDone()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	coLogFile, err := os.Create("coordinator.log")
	if err != nil {
		log.Fatalf("create log file error: %v", err)
	}
	log.SetOutput(coLogFile)
	log.SetPrefix("coordinator:")
	log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	log.Println("making coordinator")
	// Your code here.
	c.init(files, nReduce)
	c.server()
	return &c
}
