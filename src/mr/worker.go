package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	debuger "6.5840/helper"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type workerTask struct {
	files     []string
	task_type int // 0: map, 1: reduce
	task_id   int
}

var nReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(task *workerTask, mapf func(string, string) []KeyValue) (out_files []string, reduce_ids []int) {
	files := task.files

	// map task
	intermediate := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v: %v", filename, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	debuger.DPrintf("taskid: %v", task.task_id)
	debuger.DPrintf("map task %v done, eg: %v\n", task.files, intermediate[0])
	// 把task_id有的reduce文件都删除
	for i := 0; i < nReduce; i++ {
		out_file_name := fmt.Sprintf("mr-%v-%v", task.task_id, i)
		if _, err := os.Stat(out_file_name); err == nil {
			os.Remove(out_file_name)
		}
	}

	// 对中间值排序
	sort.Sort(ByKey(intermediate))
	for _, kv := range intermediate {
		reduce_id := ihash(kv.Key) % nReduce
		out_file_name := fmt.Sprintf("mr-%v-%v", task.task_id, reduce_id)
		if _, err := os.Stat(out_file_name); os.IsNotExist(err) {
			os.Create(out_file_name)
			out_files = append(out_files, out_file_name)
			reduce_ids = append(reduce_ids, reduce_id)
		}
		out_file, err := os.OpenFile(out_file_name, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open %v: %v", out_file_name, err)
		}
		json.NewEncoder(out_file).Encode(&kv) // 写入kv
		out_file.Close()
	}

	return
}

func doReduceTask(task *workerTask, reducef func(string, []string) string) {
	files := task.files
	all_keys := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			all_keys = append(all_keys, kv)
		}
	}
	sort.Sort(ByKey(all_keys))

	// 输出到reduce文件
	oname := fmt.Sprintf("mr-out-%v", task.task_id)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(all_keys) {
		j := i + 1
		for j < len(all_keys) && all_keys[j].Key == all_keys[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, all_keys[k].Value)
		}
		output := reducef(all_keys[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", all_keys[i].Key, output)
		i = j
	}
	debuger.DPrintf("reduce task %v done, data: %v\n", task.files, all_keys)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// 获取任务
		task, ret := GetTask()
		if ret == -1 {
			log.Printf("get task failed, terminate\n")
			break
		}
		if ret == 1 { // 任务被接完了
			// log.Printf("no task, sleep 10s\n")
			debuger.DPrintf("no task, sleep 10s\n")
			time.Sleep(1 * time.Second) // 太长的话parell reduce会出问题，因为没有map任务触发休息。另一个接map任务的worker处理完可能
			continue
		}
		// log.Printf("get task %v\n", task)
		debuger.DPrintf("get task %v\n", task)
		switch task.task_type {
		case 0:
			out_files, reduce_ids := doMapTask(task, mapf)
			debuger.DPrintf("map task done, out_files: %v, reduce_ids: %v\n", out_files, reduce_ids)
			// log.Printf("map task done, out_files: %v, reduce_ids: %v\n", out_files, reduce_ids)
			req := SubmitTaskArgs{task.task_type, task.task_id, out_files, reduce_ids}
			SubmitTask(req)
		case 1:
			doReduceTask(task, reducef)
			debuger.DPrintf("reduce task %v done\n", task.files)
			// log.Printf("reduce task %v done\n", task.files)
			req := SubmitTaskArgs{task.task_type, task.task_id, nil, nil}
			SubmitTask(req)
		default:
			log.Fatalf("unknown task type %v\n", task.task_type) // 貌似说这个log不好用
		}
		time.Sleep(1 * time.Second) // 帮助测并行
		// map任务就读取文件然后map
		// reduce任务就读取文件然后reduce
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetTask() (*workerTask, int) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		debuger.DPrintf("pid = %v get reply %v\n", os.Getpid(), reply)
		// fmt.Printf("reply %v\n", reply)
		if len(reply.RPC_files) == 0 {
			return nil, 1
		}
	} else {
		debuger.DPrintf("call failed!\n")
		// fmt.Printf("call failed!\n")
		return nil, -1
	}
	nReduce = reply.RPC_nReduce
	return &workerTask{reply.RPC_files, reply.RPC_task_type, reply.RPC_task_id}, 0
}

func SubmitTask(req SubmitTaskArgs) {
	reply := SubmitTaskReply{}
	ok := call("Coordinator.SubmitTask", &req, &reply)
	if ok {
		// fmt.Printf("reply %v\n", reply)
		debuger.DPrintf("reply %v\n", reply)
	} else {
		// fmt.Printf("call failed!\n")
		debuger.DPrintf("call failed!\n")
	}
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
	// log.Printf("call %v\n", rpcname)
	debuger.DPrintf("call %v\n", rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
