package mr

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

type operatorError string

func (e operatorError) Error() string {
	_, fileName, line, _ := runtime.Caller(1)
	return fmt.Sprintf("error info:%v %v \nerror message%v", fileName, line, e)
}

type node struct {
	data Task
	next *node
}
type LinkedList struct {
	head *node
	tail *node
	size int
}

func (list *LinkedList) Init() {
	list.head = &node{}
	list.tail = list.head
}

func (list *LinkedList) Size() int {
	return list.size
}

func (list *LinkedList) Pop() (Task, error) {
	if list.size == 0 {
		return Task{}, operatorError("empty queue pop!")
	}
	oldHead := list.head.next
	list.head.next = list.head.next.next
	list.size--
	return oldHead.data, nil
}

func (list *LinkedList) push(nts Task) error {
	newNode := node{}
	newNode.data = nts
	list.tail.next = &newNode
	list.tail = list.tail.next
	list.size++
	return nil
}

func (tsq *SyncedQueue) Init() {
	tsq.taskArray.Init()
}

type SyncedQueue struct {
	taskArray LinkedList
	mutex     sync.Mutex
}

func (tsq *SyncedQueue) Size() int {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	return tsq.taskArray.Size()
}

func (tsq *SyncedQueue) Pop() (Task, error) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	ret, err := tsq.taskArray.Pop()
	return ret, err
}

func (tsq *SyncedQueue) push(nts Task) error {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	return tsq.taskArray.push(nts)
}

type SyncedSet struct {
	set   map[int]Task
	mutex sync.Mutex
}

func (tss *SyncedSet) Init() {
	tss.set = make(map[int]Task)
}

func (tss *SyncedSet) Size() int {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()
	return len(tss.set)
}

func (tss *SyncedSet) insert(tsk Task) error {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()
	tss.set[tsk.taskId] = tsk
	return nil
}

func (tss *SyncedSet) find(tskId int) bool {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()
	_, ok := tss.set[tskId]
	return ok
}

func (tss *SyncedSet) get() (Task, error) {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()
	tskId := rand.Intn(len(tss.set))
	res, ok := tss.set[tskId]
	if ok {
		return res, nil
	}
	return res, operatorError("no taskId")
}

func (tss *SyncedSet) remove(tskId int) error {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()
	delete(tss.set, tskId)
	return nil
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task struct {
	beginTime     time.Time
	outTime       time.Duration
	nReduce       int
	taskId        int
	taskType      int
	inputFileList []string
}

type TaskInfo struct {
	state         int
	taskId        int
	beginTime     time.Time
	outTime       time.Duration
	nReduce       int
	inputFileList []string
}

type Reply struct {
	Done           bool
	OutPutFileList []string
}

func (t Task) GenerateTaskInfo() TaskInfo {
	info := TaskInfo{
		taskId:        t.taskId,
		state:         t.taskType,
		beginTime:     t.beginTime,
		outTime:       t.outTime,
		nReduce:       t.nReduce,
		inputFileList: make([]string, len(t.inputFileList)),
	}
	copy(info.inputFileList, t.inputFileList)
	return info
}

func (this Task) TaskId() int {
	return this.taskId
}

func (this Task) OutOfTime() bool {
	return time.Now().After(this.beginTime.Add(this.outTime))
}

const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	TaskId() int
	OutOfTime() bool
	//GetFileIndex() bool
	//GetPartIndex() int
	//SetNow()
}
