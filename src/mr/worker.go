package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type mrWorker struct {
	// Map function provided by user.
	mapf func(string, string) []KeyValue

	// Reduce function provided by user.
	reducef func(string, []string) string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(io.Discard)
	log.Printf("Start worker of %+v", os.Getpid())

	worker := &mrWorker{mapf: mapf, reducef: reducef}

	for {
		task := tryContinueRequestMasterForMRTask(worker)

		switch task.TaskType {
		case MapTask:
			if err := worker.handleMapTask(*task); err != nil {
				log.Fatal(err)
			}
		case ReduceTask:
			if err := worker.handleReduceTask(*task); err != nil {
				log.Fatal(err)
			}
		default:
			log.Fatal("unknown task type")
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (w mrWorker) handleReduceTask(task Task) error {
	reduceInputFiles := make([]*os.File, 0)
	for _, reduceInputFileName := range task.InputReduceFileNames {
		reduceInputFile, err := os.Open(reduceInputFileName)
		if err != nil {
			return err
		}
		defer tryCloseFile(reduceInputFile)
		reduceInputFiles = append(reduceInputFiles, reduceInputFile)
	}

	intermediate := readAndSortKeyValuesFromNReduceFile(reduceInputFiles)

	reduceOutTempFile, err := tryCreateReduceOutTempFileInCurrentDir(
		task.NReduce)
	if err != nil {
		return err
	}

	if err = w.callReduceAndWriteToTempFile(intermediate,
		reduceOutTempFile); err != nil {
		return err
	}

	if err := os.Rename(reduceOutTempFile.Name(),
		reduceOutFileName(task.InputReduceFileIndex)); err != nil {
		return err
	}
	if err = w.tryReportReduceTaskDone(task.TaskId); err != nil {
		return err
	}

	return nil
}

func (w mrWorker) callReduceAndWriteToTempFile(intermediate []KeyValue,
	reduceOutTempFile *os.File) error {
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		if _, err := fmt.Fprintf(reduceOutTempFile, "%v %v\n",
			intermediate[i].Key, output); err != nil {
			return err
		}

		i = j
	}
	return nil
}

func readAndSortKeyValuesFromNReduceFile(reduceInputFiles []*os.File) []KeyValue {
	var intermediate []KeyValue

	for _, reduceInputFile := range reduceInputFiles {
		scanner := bufio.NewScanner(reduceInputFile)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.TrimSpace(line) == "" {
				continue
			}

			keyValuePairs := strings.SplitN(line, " ", 2)
			intermediate = append(intermediate,
				KeyValue{Key: keyValuePairs[0], Value: keyValuePairs[1]})
		}
	}

	sort.Sort(ByKey(intermediate))
	return intermediate
}

func tryCloseFile(file *os.File) {
	func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}(file)
}

func (w mrWorker) handleMapTask(task Task) error {
	intermediateKeyVales, err := w.callMapOnFile(task)
	if err != nil {
		return err
	}

	mapOutFiles, err := w.hashToMapOutFiles(task, intermediateKeyVales)
	if err != nil {
		return err
	}

	if err = w.tryReportMapTaskDone(task.TaskId, mapOutFiles); err != nil {
		return err
	}

	return nil
}

func (w mrWorker) hashToMapOutFiles(task Task, intermediateKeyVales []KeyValue) ([]string, error) {
	mapOutTempFiles, err := tryCreateMapOutTempFilesInCurrentDir(task)
	if err != nil {
		return nil, err
	}

	if err = w.hashKeysToMapOutTempFile(task.NReduce, intermediateKeyVales,
		mapOutTempFiles); err != nil {
		return nil, err
	}

	mapOutFiles, err := atomicRenameTempFilesToMapOutFile(task,
		mapOutTempFiles)
	if err != nil {
		return nil, err
	}
	return mapOutFiles, nil
}

func atomicRenameTempFilesToMapOutFile(task Task,
	mapOutTempFiles []*os.File) ([]string, error) {
	mapOutFileNames := make([]string, task.NReduce)

	for nReduceIndex, mapOutTempFile := range mapOutTempFiles {
		if err := mapOutTempFile.Close(); err != nil {
			return nil, err
		}
		outFileName := mapOutFileName(task.InputMapFileIndex, nReduceIndex)
		if err := os.Rename(mapOutTempFile.Name(), outFileName); err != nil {
			return nil, err
		}

		mapOutFileNames[nReduceIndex] = outFileName
	}

	return mapOutFileNames, nil
}

func (w mrWorker) hashKeysToMapOutTempFile(nReduce int,
	intermediateKeyVales []KeyValue, mapOutTempFiles []*os.File) error {
	for _, kv := range intermediateKeyVales {
		reduceIndex := w.ihash(kv.Key) % nReduce
		if _, err := mapOutTempFiles[reduceIndex].WriteString(
			fmt.Sprintf("%s %s\n", kv.Key, kv.Value)); err != nil {
			return err
		}
	}
	return nil
}

func tryCreateMapOutTempFilesInCurrentDir(task Task) (
	[]*os.File, error) {
	mapOutTempFiles := make([]*os.File, task.NReduce)
	var err error

	for reduceIndex := 0; reduceIndex < task.NReduce; reduceIndex++ {
		mapOutTempFiles[reduceIndex], err = tryCreateMapOutTempFileInCurrentDir(
			task, reduceIndex)
		if err != nil {
			return nil, err
		}
	}

	return mapOutTempFiles, nil
}

func tryCreateMapOutTempFileInCurrentDir(task Task, reduceIndex int) (
	*os.File, error) {
	currentWorkDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	mapOutTempFile, err := os.CreateTemp(currentWorkDir,
		mapOutFileName(task.InputMapFileIndex, reduceIndex))
	if err != nil {
		return nil, err
	}
	return mapOutTempFile, nil
}

func tryCreateReduceOutTempFileInCurrentDir(nReduce int) (*os.File, error) {
	currentWorkDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	reduceOutTempFile, err := os.CreateTemp(currentWorkDir,
		reduceOutFileName(nReduce))
	if err != nil {
		return nil, err
	}
	return reduceOutTempFile, nil
}

func mapOutFileName(mapFIleIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d",
		mapFIleIndex, reduceIndex)
}

func reduceOutFileName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

func (w mrWorker) callMapOnFile(task Task) ([]KeyValue, error) {
	fileBytes, err := os.ReadFile(task.InputMapFileName)
	if err != nil {
		return nil, err
	}

	return w.mapf(task.InputMapFileName, string(fileBytes)), nil
}

func tryContinueRequestMasterForMRTask(worker *mrWorker) *Task {
	task := worker.tryRequestMasterForMRTask()

	for task.TaskType == CurrentNoTask {
		time.Sleep(500 * time.Millisecond)
		task = worker.tryRequestMasterForMRTask()
	}

	log.Printf("Worker gets task of %+v\n", task)
	return task
}

func (w mrWorker) tryRequestMasterForMRTask() *Task {
	task, err := w.requestMasterForTask()
	if err != nil {
		log.Fatal(err)
	}

	if task.TaskType == Done {
		log.Println("All tasks done, worker finished.")
		os.Exit(0)
	}
	return task
}

func (w mrWorker) tryReportMapTaskDone(taskId int, mapOutFiles []string) error {
	if (!w.callMaster("Coordinator.ReceiveMapTaskDone",
		&MapTaskDone{taskId, mapOutFiles}, &Response{})) {
		return errors.New("cannot report map task done to master")
	}
	return nil
}

func (w mrWorker) tryReportReduceTaskDone(taskId int) error {
	if (!w.callMaster("Coordinator.ReceiveReduceTaskDone",
		&ReduceTaskDone{taskId}, &Response{})) {
		return errors.New("cannot report map task done to master")
	}
	return nil
}

func (w mrWorker) requestMasterForTask() (*Task, error) {
	task := Task{}

	if (!w.callMaster("Coordinator.PollTask", &RequestTask{}, &task)) {
		return nil, errors.New("request task failed")
	}

	return &task, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w mrWorker) callMaster(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Printf("close rpc client error: %v\n", err)
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func (w mrWorker) ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}
