package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	nReduce int

	checkWorkDuration time.Duration

	taskQueue *list.List

	runningTaskWorkerMap map[string]*runningTask

	runningTaskWorkerTimerMap map[string]*time.Timer

	reduceIndex int

	mutex sync.Mutex
}

type runningTask struct {
	task *Task

	startTime int64

	workerId string
}

// start a thread that listens for RPCs from mrWorker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("register error:", err)
	}

	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
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

	return c.taskQueue.Len() == 0 && len(c.runningTaskWorkerMap) == 0
}

func (c *Coordinator) ReceiveMapTaskDone(taskDone *MapTaskDone) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.runningTaskWorkerMap[taskDone.WorkerId] != nil {
		log.Println("The task has been done.")
		return
	}

	c.clearTask(taskDone.WorkerId)
	c.addReduceTaskFromMapTask(*taskDone)
}

func (c *Coordinator) ReceiveReduceTaskDone(taskDone *ReduceTaskDone) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.runningTaskWorkerMap[taskDone.WorkerId] != nil {
		log.Println("The task has been done.")
		return
	}

	c.clearTask(taskDone.WorkerId)
}

func (c *Coordinator) clearTask(workerId string) {
	delete(c.runningTaskWorkerMap, workerId)

	timer := c.runningTaskWorkerTimerMap[workerId]
	timer.Stop()
	delete(c.runningTaskWorkerTimerMap, workerId)
}

func (c *Coordinator) PollTask(requestTask *RequestTask, task *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.taskQueue.Len() == 0 {
		task.TaskType = CurrentNoTask
		return nil
	}

	newTask := c.popFrontTask()
	task.SetFrom(newTask)

	c.addRunningTask(*requestTask, newTask)
	c.registerCheckTimer(*requestTask)

	return nil
}

func (c *Coordinator) registerCheckTimer(requestTask RequestTask) {
	timer := time.AfterFunc(c.checkWorkDuration, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		runningTask := c.runningTaskWorkerMap[requestTask.WorkerId]

		if runningTask != nil {
			delete(c.runningTaskWorkerMap, requestTask.WorkerId)
			delete(c.runningTaskWorkerTimerMap, requestTask.WorkerId)
			c.taskQueue.PushBack(runningTask.task)
		}
	})
	c.runningTaskWorkerTimerMap[requestTask.WorkerId] = timer
}

func (c *Coordinator) addReduceTaskFromMapTask(mapTaskDone MapTaskDone) {
	for i := 0; i < len(mapTaskDone.mapOutFiles); i++ {
		reduceTask := Task{
			FileName:        mapTaskDone.mapOutFiles[i],
			NReduce:         c.nReduce,
			ReduceFileIndex: c.reduceIndex,
			TaskType:        ReduceTask,
		}
		c.taskQueue.PushBack(&reduceTask)
		c.reduceIndex++
	}
}

func (c *Coordinator) addRunningTask(requestTask RequestTask, task *Task) {
	c.runningTaskWorkerMap[requestTask.WorkerId] = &runningTask{
		task:      task,
		startTime: time.Now().UnixMilli(),
		workerId:  requestTask.WorkerId,
	}
}

func (c *Coordinator) popFrontTask() *Task {
	front := c.taskQueue.Front()
	c.taskQueue.Remove(front)
	return front.Value.(*Task)
}

func (c *Coordinator) initMapTask(originalFileNames []string) {
	for index, originalFileName := range originalFileNames {
		c.taskQueue.PushBack(&Task{
			TaskType:     MapTask,
			MapFileIndex: index,
			FileName:     originalFileName,
			NReduce:      c.nReduce,
		})
	}
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:                   nReduce,
		checkWorkDuration:         10 * time.Second,
		taskQueue:                 list.New(),
		runningTaskWorkerMap:      make(map[string]*runningTask),
		runningTaskWorkerTimerMap: make(map[string]*time.Timer),
		reduceIndex:               0,
		mutex:                     sync.Mutex{},
	}
	c.initMapTask(files)

	c.server()
	return &c
}
