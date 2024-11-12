package mr

//
// RPC definitions.
//

type RequestTask struct {
	WorkerId string
}

type MapTaskDone struct {
	WorkerId string

	mapOutFiles []string
}

type ReduceTaskDone struct {
	WorkerId string
}

type Task struct {
	TaskType TaskType

	MapFileIndex int

	ReduceFileIndex int

	FileName string

	NReduce int
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	CurrentNoTask
	Done
)

func (task *Task) SetFrom(newTask *Task) {
	task.FileName = newTask.FileName
	task.MapFileIndex = newTask.MapFileIndex
	task.NReduce = newTask.NReduce
	task.ReduceFileIndex = newTask.ReduceFileIndex
	task.TaskType = newTask.TaskType
}
