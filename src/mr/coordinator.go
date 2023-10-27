package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	debuger "6.5840/helper"
)

type Task struct {
	files []string
	// 0: idle
	// 1: in-progress
	// 2: completed
	status    int
	timestamp int64 // 开启的时间戳
}

type Coordinator struct {
	// map task列表
	map_tasks []Task
	// reduce task列表
	reduce_tasks     []Task
	valid_reduce_cnt int
	// 划分多少reduce
	nReduce int
	// 处理好的文件数量
	done_map_task    int
	done_reduce_task int
	mu               sync.Mutex

	is_done bool
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 1.分配任务
// map task直接返回文件列表(方便多个节点读)，reduce task通过信号量阻塞。
// 所有map上报后，reduce task得到列表
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done_map_task != len(c.map_tasks) {
		// get map task
		c.getMapTask(args, reply)
	} else {
		// get reduce task
		debuger.DPrintf("get reduce task\n")
		c.getReduceTask(args, reply)
	}
	// log.Printf("GetTask reply %v\n", *reply)
	debuger.DPrintf("GetTask reply %v\n", *reply)
	return nil
}

func (c *Coordinator) getMapTask(args *GetTaskArgs, reply *GetTaskReply) int {
	reply.RPC_task_type = 0
	for i := range c.map_tasks {
		task := &c.map_tasks[i]
		if task.status == 0 {
			// 未分配任务
			task.status = 1
			task.timestamp = time.Now().Unix()

			reply.RPC_nReduce = c.nReduce
			reply.RPC_files = append(reply.RPC_files, task.files...)
			reply.RPC_task_type = 0
			reply.RPC_task_id = i // map task本地输出mr-X-Y文件，X为task_id, Y为reduce_id
			break
		}
	}
	return 0
}

func (c *Coordinator) getReduceTask(args *GetTaskArgs, reply *GetTaskReply) int {
	reply.RPC_task_type = 1
	for i := range c.reduce_tasks {
		task := &c.reduce_tasks[i]
		if task.status == 0 && len(task.files) > 0 {
			// 未分配任务
			task.status = 1
			task.timestamp = time.Now().Unix()

			reply.RPC_files = append(reply.RPC_files, task.files...)
			reply.RPC_task_type = 1
			reply.RPC_task_id = i
			break
		}
	}
	return 0
}

// 2.上报完成map task任务和reduce列表
// 所有map task完成后，分配reduce task任务
func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	debuger.DPrintf("SubmitTask args %v\n", *args)
	if args.RPC_task_type == 0 {
		if c.map_tasks[args.RPC_task_id].status == 2 {
			return nil
		}
		c.map_tasks[args.RPC_task_id].status = 2
		c.done_map_task++
		for i, reduce_id := range args.RPC_reduce_id {
			if len(args.RPC_files) != 0 && len(c.reduce_tasks[reduce_id].files) == 0 {
				c.valid_reduce_cnt++
			}
			c.reduce_tasks[reduce_id].files = append(c.reduce_tasks[reduce_id].files, args.RPC_files[i])
		}
	} else if args.RPC_task_type == 1 {
		if c.reduce_tasks[args.RPC_task_id].status == 2 {
			return nil
		}
		c.reduce_tasks[args.RPC_task_id].status = 2
		c.done_reduce_task++
		// 有效的reduce task
		debuger.DPrintf("done_reduce_task: %v, valid_reduce_cnt %v\n", c.done_reduce_task, c.valid_reduce_cnt)
		if c.done_reduce_task == c.valid_reduce_cnt {
			// 任务完成，通知结束
			c.is_done = true
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() { // 本地测试，还挺爽
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// 保活: 遍历每个任务的时间戳，超过10s就重新分配
//
func (c *Coordinator) keepAlive() {
	for {
		if c.is_done {
			break
		}
		// 检测时间戳
		{
			c.mu.Lock()
			if c.done_map_task != len(c.map_tasks) {
				for i, task := range c.map_tasks {
					if task.status == 1 && time.Now().Unix()-task.timestamp > 10 {
						// 重新分配任务
						c.map_tasks[i].status = 0
					}
				}
			} else {
				for i, task := range c.reduce_tasks {
					if task.status == 1 && time.Now().Unix()-task.timestamp > 10 {
						// 重新分配任务
						c.reduce_tasks[i].status = 0
					}
				}
			}
			c.mu.Unlock()
		}
		time.Sleep(10 * time.Second)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.is_done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 1.初始化map task列表
	for _, file := range files {
		c.map_tasks = append(c.map_tasks, Task{[]string{file}, 0, 0})
	}
	log.Printf("map tasks: %v\n", c.map_tasks)
	c.reduce_tasks = make([]Task, nReduce)
	c.nReduce = nReduce

	// 再启动一个线程保活吧，根据启动的10s时间戳，如果超过10s，就重新分配任务
	go c.keepAlive()

	c.server()
	return &c
}
