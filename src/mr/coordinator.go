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

	mutex sync.Mutex
}

type runningTask struct {
	task *Task

	startTime int64

	workerId string
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.taskQueue.Len() == 0 && len(c.runningTaskWorkerMap) == 0
}

func (c *Coordinator) taskDone(taskDone TaskDone) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.runningTaskWorkerMap, taskDone.WorkerId)

	timer := c.runningTaskWorkerTimerMap[taskDone.WorkerId]
	timer.Stop()
	delete(c.runningTaskWorkerTimerMap, taskDone.WorkerId)
}

func (c *Coordinator) PollTask(requestTask RequestTask) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.taskQueue.Len() == 0 {
		return &Task{TaskType: NoTask}
	}

	task := c.popFrontTask()

	c.addRunningTask(requestTask, task)
	c.registerCheckTimer(requestTask)

	return task
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
	for _, originalFileName := range originalFileNames {
		c.taskQueue.PushBack(&Task{
			TaskType: MapTask,
			FileName: originalFileName,
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
		mutex:                     sync.Mutex{},
	}
	c.initMapTask(files)

	c.server()
	return &c
}
