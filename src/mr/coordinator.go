package mr

import (
	"container/list"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	checkWorkDuration time.Duration

	nReduce        int
	splitFileCount int

	idleTaskQueue       *list.List
	runningTaskMap      map[int]*runningTask
	runningTaskTimerMap map[int]*time.Timer

	completedMapTasks []*MapTaskDone

	currentTaskId int

	mutex sync.Mutex
}

type runningTask struct {
	task *Task

	startTime int64
}

// start a thread that listens for RPCs from mrWorker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("register error:", err)
	}
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("Serve error:", err)
		}
	}()
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.done()
}

func (c *Coordinator) done() bool {
	return c.idleTaskQueue.Len() == 0 && len(c.runningTaskMap) == 0
}

func (c *Coordinator) ReceiveMapTaskDone(taskDone *MapTaskDone, response *Response) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.runningTaskMap[taskDone.TaskId] == nil {
		log.Printf("Task of %d has been done, don't need to clear meta data.", taskDone.TaskId)
		return nil
	}

	log.Printf("Map Task of %d is done.\n", taskDone.TaskId)

	c.clearTask(taskDone.TaskId)
	c.addCompletedMapTask(*taskDone)
	c.tryAddReduceTasks()

	return nil
}

func (c *Coordinator) ReceiveReduceTaskDone(taskDone *ReduceTaskDone, response *Response) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.runningTaskMap[taskDone.TaskId] == nil {
		log.Printf("Task of %d has been done, don't need to clear meta data.", taskDone.TaskId)
		return nil
	}

	log.Printf("Reduce task of %d is done.\n", taskDone.TaskId)

	c.clearTask(taskDone.TaskId)
	return nil
}

func (c *Coordinator) clearTask(taskId int) {
	delete(c.runningTaskMap, taskId)

	timer := c.runningTaskTimerMap[taskId]
	timer.Stop()
	delete(c.runningTaskTimerMap, taskId)
}

func (c *Coordinator) PollTask(requestTask *RequestTask, task *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.done() {
		task.TaskType = Done
		log.Printf("All tasks has been done.\n")
		return nil
	}

	if c.idleTaskQueue.Len() == 0 {
		task.TaskType = CurrentNoTask
		return nil
	}

	newTask := c.popFrontTask()
	task.SetFrom(newTask)

	c.addRunningTask(newTask)
	c.registerCheckTimer(newTask.TaskId)

	log.Printf("Master offer task of %+v", task)

	return nil
}

func (c *Coordinator) registerCheckTimer(taskId int) {
	timer := time.AfterFunc(c.checkWorkDuration, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		runningTask := c.runningTaskMap[taskId]

		if runningTask != nil {
			log.Printf("%+v Task of %d is expired, push back to idle task queue\n",
				runningTask.task.TaskType, taskId)
			delete(c.runningTaskMap, taskId)
			delete(c.runningTaskTimerMap, taskId)
			c.idleTaskQueue.PushBack(runningTask.task)
		}
	})
	c.runningTaskTimerMap[taskId] = timer
}

func (c *Coordinator) addCompletedMapTask(mapTaskDone MapTaskDone) {
	c.completedMapTasks = append(c.completedMapTasks, &mapTaskDone)
}

func (c *Coordinator) tryAddReduceTasks() {
	if len(c.completedMapTasks) != c.splitFileCount &&
		len(c.completedMapTasks) > 0 {
		return
	}

	reduceTasks := make([]*Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		reduceTasks[i] = &Task{
			NReduce:              c.nReduce,
			TaskType:             ReduceTask,
			TaskId:               c.nextTaskId(),
			InputReduceFileIndex: i,
			InputReduceFileNames: make([]string, 0),
		}
	}

	for _, completedMapTask := range c.completedMapTasks {
		for rFileIndx, rFileName := range completedMapTask.MapOutFiles {
			reduceTasks[rFileIndx].InputReduceFileNames = append(
				reduceTasks[rFileIndx].InputReduceFileNames, rFileName)
		}
	}

	for _, reduceTask := range reduceTasks {
		c.idleTaskQueue.PushBack(reduceTask)
	}

	clear(c.completedMapTasks)
}

func (c *Coordinator) addRunningTask(task *Task) {
	c.runningTaskMap[task.TaskId] = &runningTask{
		task:      task,
		startTime: time.Now().UnixMilli(),
	}
}

func (c *Coordinator) popFrontTask() *Task {
	front := c.idleTaskQueue.Front()
	c.idleTaskQueue.Remove(front)
	return front.Value.(*Task)
}

func (c *Coordinator) initMapTask(originalFileNames []string) {
	log.Printf("Init all map tasks for: %+v", originalFileNames)

	for index, originalFileName := range originalFileNames {
		c.idleTaskQueue.PushBack(&Task{
			TaskType:          MapTask,
			TaskId:            c.nextTaskId(),
			InputMapFileIndex: index,
			InputMapFileName:  originalFileName,
			NReduce:           c.nReduce,
		})
	}
}

func (c *Coordinator) nextTaskId() int {
	c.currentTaskId += 1
	return c.currentTaskId
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetOutput(io.Discard)
	c := Coordinator{
		splitFileCount:      len(files),
		nReduce:             nReduce,
		checkWorkDuration:   10 * time.Second,
		idleTaskQueue:       list.New(),
		runningTaskMap:      make(map[int]*runningTask),
		runningTaskTimerMap: make(map[int]*time.Timer),
		mutex:               sync.Mutex{},
	}
	c.initMapTask(files)

	c.server()
	return &c
}
