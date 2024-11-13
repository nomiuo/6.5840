package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//

type RequestTask struct {
}

type MapTaskDone struct {
	TaskId int

	MapOutFiles []string
}

type ReduceTaskDone struct {
	TaskId int
}

type Task struct {
	NReduce int

	TaskType TaskType

	TaskId int

	InputMapFileIndex int

	InputMapFileName string

	InputReduceFileIndex int

	InputReduceFileNames []string
}

type Response struct {
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	CurrentNoTask
	Done
)

func (task *Task) SetFrom(newTask *Task) {
	task.NReduce = newTask.NReduce
	task.TaskId = newTask.TaskId
	task.InputMapFileName = newTask.InputMapFileName
	task.InputMapFileIndex = newTask.InputMapFileIndex
	task.InputReduceFileIndex = newTask.InputReduceFileIndex
	task.InputReduceFileNames = newTask.InputReduceFileNames
	task.TaskType = newTask.TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
