package mr

//
// RPC definitions.
//

type RequestTask struct {
	WorkerId string
}

type TaskDone struct {
	WorkerId string
}

type Task struct {
	TaskType TaskType

	FileName string
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
)
