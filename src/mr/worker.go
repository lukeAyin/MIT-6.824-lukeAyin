package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	log.SetPrefix("worker" + strconv.Itoa(os.Getpid()) + ":")
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	workLogFile, err := os.Create("worker.log")
	if err != nil {
		log.Fatalf("create worker log file failed! error:,%v", err)
	}
	log.SetOutput(workLogFile)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		log.Println("worker ready to call a task with coordinator")
		taskInfo := CallForTask()
		switch taskInfo.state {
		case TaskMap:
			log.Println("call for a map task")
			workerMap(mapf, *taskInfo)
			break
		case TaskReduce:
			log.Println("call for a reduce task")
			workerReduce(reducef, *taskInfo)
			break
		case TaskWait:
			log.Println("no task now, waiting 5 seconds")
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			log.Println("coordinator all tasks complete,nothing to do")
			break
		default:
			panic("Invalid task state received by worker")
		}
		if taskInfo.state == TaskEnd {
			break
		}
	}
}
func workerMap(mapf func(string, string) []KeyValue, taskInfo TaskInfo) {
	intermediate := []KeyValue{}
	if len(taskInfo.inputFileList) < 1 {
		log.Fatalf("map task %v inputFileList length less 1", taskInfo.taskId)
	}
	file, err := os.Open(taskInfo.inputFileList[0])
	if err != nil {
		log.Fatalf("cannot open %v ", taskInfo.inputFileList[0])
	}
	content, err := io.ReadAll(file)
	fileName := file.Name()
	if err != nil {
		log.Fatalf("cannot Read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	nReduce := taskInfo.nReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outIndex := 0; outIndex < nReduce; outIndex++ {
		outFiles[outIndex], err = os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("create %vth tmp file failed error:%v", outIndex, err)
		}
		fileEncs[outIndex] = json.NewEncoder(outFiles[outIndex])
	}
	for _, kv := range intermediate {
		outIndex := ihash(kv.Key) % nReduce
		file = outFiles[outIndex]
		enc := fileEncs[outIndex]
		err := enc.Encode(kv)
		if err != nil {
			log.Fatalf("error type: %T", err)
			log.Fatalf("File:%v key:%v value:%v error:%v \n", fileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	outFileNames := make([]string, nReduce)
	for outIndex, file := range outFiles {
		outFileNames[outIndex] = outFiles[outIndex].Name()
		file.Close()
	}
	reply := Reply{
		Done:           true,
		OutPutFileList: outFileNames,
	}
	callTaskDone(taskInfo, reply)
}
func workerReduce(reducef func(string, []string) string, taskInfo TaskInfo) {
	intermediate := []KeyValue{}
	for index := 0; index < len(taskInfo.inputFileList); index++ {
		inname := taskInfo.inputFileList[index]
		file, err := os.Open(inname)
		if err != nil {
			log.Fatalf("open intermediate file %v failed : %v \n", inname, err)
			panic("open file error")
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				log.Fatalf("decode intermediate file %v failed : %v \n", inname, err)
				break
			} else {
				intermediate = append(intermediate, kv)
			}
		}
	}
	log.Printf("reduce task %v decode success!", taskInfo.taskId)
	sort.Sort(ByKey(intermediate))

	ofile, err := os.CreateTemp(".", "mr-*")
	if err != nil {
		fmt.Printf("create output tmp file failed: %v \n", err)
	}
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
		fmt.Fprintf(ofile, "%v %v \n", intermediate[i].Key, output)
		i = j
	}
	reply := Reply{
		Done: true,
	}
	reply.OutPutFileList = append(reply.OutPutFileList, ofile.Name())
	callTaskDone(taskInfo, reply)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func CallForTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Coordinator.AskTask", &args, &reply)
	return &reply
}
func callTaskDone(taskInfo TaskInfo, reply Reply) {
	call("Coordinator.TaskDone", &taskInfo, &reply)
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
