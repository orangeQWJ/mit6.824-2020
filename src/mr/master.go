package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// map/Reduce任务所处的状态常量
type TaskState int

const (
	NotAssigned TaskState = iota
	Running
	Completed
)

type TaskStatus struct {
	State     TaskState
	StartTime time.Time //任务开始的时间,超时依据
}

type MasterProgress int

const (
	Mapping MasterProgress = iota
	Reducing
	Done
)

type Master struct {
	// Your definitions here.
	mutex             sync.Mutex     // 一把大锁保平安
	intermediateFiles [][]string     // map任务产生中间文件的位置
	mapTasks          []TaskStatus   // 每个map任务的执行情况
	reduceTasks       []TaskStatus   // 每个reduce任务的执行情况
	filesForMap       []string       // 每个map任务的输入文件的位置
	filesByReduce     []string       // 每个reduce任务产生的最终文件的位置
	nMap              int            // map任务的数量
	nReduce           int            // reduce任务的数量
	progress          MasterProgress // 当前任务所处的大阶段
}
type taskType int

const (
	Map taskType = iota
	Reduce
	Wait
	Over
)

// 派发任务和回报任务复用同一个数据结构
type Task struct {
	TaskType taskType
	// map任务根据M任务号组成中间文件名的一部分,
	// map/reduce任务通过任务号告诉master自己完成了哪个任务
	ID int
	// map任务需要一个文件,reduce任务需要nReduce个文件
	// map任务报告返回nReduce个文件,reduce任务报告返回1个文件
	Files   []string
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) HeartBeat(_ struct{}, t *Task) error {
	*t = *(m.assignTask())
	return nil
}

// master 分配一个任务
func (m *Master) assignTask() *Task {
	//logDebugInfo("assignTask 尝试获取锁\n")
	m.mutex.Lock()
	//logDebugInfo("assignTask 获取了锁!!!\n")
	defer func() {
		//logDebugInfo("assignTask 释放了锁!!!\n")
		m.mutex.Unlock()
	}()
	switch m.progress {
	case Mapping:
		//找一个未分配的Map任务
		for i := 0; i < m.nMap; i++ {
			if m.mapTasks[i].State == NotAssigned {
				m.mapTasks[i].State = Running
				m.mapTasks[i].StartTime = time.Now()
				t := &Task{
					TaskType: Map,
					ID:       i,
					NReduce:  m.nReduce,
					Files:    []string{m.filesForMap[i]},
				}
				//logDebugInfo("分配任务")
				//fmt.Println(*t)
				return t
			}
		}
		// Master.progress == Mapping 但是找不到一个未分配的Map任务
		// 当所有map任务都已分配,但是还有map任务未完成
		// 分配给worker一个等待任务,让他等会再来问问
		return &Task{
			TaskType: Wait,
		}
	case Reducing:
		// 处在Reducing状态时,所有的reduce task都处于可分配状态
		//找一个未分配的Reduce任务
		for j := 0; j < m.nReduce; j++ {
			if m.reduceTasks[j].State == NotAssigned {
				m.reduceTasks[j].State = Running
				m.reduceTasks[j].StartTime = time.Now()
				files := []string{}
				for i := 0; i < m.nMap; i++ {
					files = append(files, m.intermediateFiles[i][j])
				}
				t := &Task{
					TaskType: Reduce,
					ID:       j,
					Files:    files,
					NReduce:  m.nReduce,
				}
				return t
			}
		}
		// Master.progress == Reducing 但是找不到一个未分配的Reduce任务
		// 当所有reduce任务都已分配,但是还有reduce任务未完成
		// 分配给worker一个等待任务,让他等会再来问问
		//		logDebugInfo("所有的reduce任务都分配完了")
		return &Task{
			TaskType: Wait,
			//TaskType: Over,
		}
	case Done:
		//log.Printf("所有任务都结束了")
		return &Task{
			TaskType: Over,
		}
	default:
		panic("master处于一个未定义的状态")
	}
}

// Task 任务返回一堆中间文件的path
// reduce 任务返回
// 信息从worker单向流向master
func (m *Master) ReceiveTaskCompletionReport(t *Task, _ *Task) error {
	//logDebugInfo("ReceiveTaskCompletionReport 尝试获取锁")
	m.mutex.Lock()
	//logDebugInfo("ReceiveTaskCompletionReport 尝试了锁!!!")
	defer func() {
		m.mutex.Unlock()
		//logDebugInfo("ReceiveTaskCompletionReport 释放了锁!!!")
	}()
	switch t.TaskType {
	case Map:
		m.mapTasks[t.ID].State = Completed
		m.intermediateFiles[t.ID] = t.Files
		/*
			for j:= 0 ;j < m.nReduce ;j ++ {
				fmt.Println(m.intermediateFiles[t.ID][j])
			}
		*/
	case Reduce:
		m.reduceTasks[t.ID].State = Completed
		m.filesByReduce[t.ID] = t.Files[0]
	default:
		panic("任务完成报告有问题")
	}
	return nil
}

func (m *Master) updateMasterStatus() {
	// 当map任务都完成时,将Master.progress 设为Reducing
	// 当reduce任务都完成时,将Master.progress 设为Done
	// 当Master.progesss 循环结束
	for {
		// 降低性能损耗
		time.Sleep(3 * time.Second)
		//logDebugInfo("updateMasterStatus试图获取锁\n")
		m.mutex.Lock()
		//logDebugInfo("updateMasterStatus获取锁!!!!!\n")
		switch m.progress {
		case Mapping:
			mapTasksDone := true
			for _, v := range m.mapTasks {
				if v.State != Completed {
					mapTasksDone = false
				}
			}
			if mapTasksDone {
				m.progress = Reducing
			}
		case Reducing:
			reduceTasksDone := true
			for _, v := range m.reduceTasks {
				if v.State != Completed {
					reduceTasksDone = false
				}
			}
			if reduceTasksDone {
				m.progress = Done
			}
		case Done:
			// 任务结束,停止更新状态
			//logDebugInfo("updateMasterStatus 函数结束")
			return
		default:
			panic("更新master总任务进展出错")
		}
		m.mutex.Unlock()
		//logDebugInfo("updateMasterStatus释放了锁!!!!!\n")

		/*
			for i, v := range m.mapTasks {
				fmt.Printf("map[%d]:%d	", i, v.State)
				if i%4 == 0 {
					fmt.Println()
				}
			}
			for i, v := range m.reduceTasks {
				fmt.Printf("reduce[%d]:%d	", i, v.State)
				if i%4 == 0 {
					fmt.Println()
				}
			}
			log.Printf("Master.progress[%d]\n", m.progress)
		*/
	}
}

func (m *Master) CheckWorkerAlive() {
	for {
		time.Sleep(1 * time.Second)
		//logDebugInfo("CheckWorkerAlive 尝试获取锁")
		m.mutex.Lock()
		//logDebugInfo("CheckWorkerAlive 获取了锁!!!!")
		switch m.progress {
		case Mapping:
			for i := 0; i < m.nMap; i++ {
				if m.mapTasks[i].State == Running {
					duration := time.Now().Sub(m.mapTasks[i].StartTime)
					if duration > 10*time.Second {
						m.mapTasks[i].State = NotAssigned
					}
				}
			}
		case Reducing:
			for i := 0; i < m.nReduce; i++ {
				if m.reduceTasks[i].State == Running {
					duration := time.Now().Sub(m.reduceTasks[i].StartTime)
					if duration > 10*time.Second {
						m.reduceTasks[i].State = NotAssigned
					}
				}
			}
		case Done:
			m.mutex.Unlock()
			return
		}
		m.mutex.Unlock()
		//logDebugInfo("CheckWorkerAlive 释放了锁!!!!")
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	if m.progress == Done {
		return true
	} else {
		return false
	}
	//ret := false

	// Your code here.

	//return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMap:     len(files),
		nReduce:  nReduce,
		progress: Mapping,
	}
	// 之后对intermediateFiles的访问不是通过append
	// 需要先明确长度,否则索引越界
	m.intermediateFiles = make([][]string, m.nMap)
	for i := range m.intermediateFiles {
		m.intermediateFiles[i] = make([]string, m.nReduce)
	}
	m.mapTasks = make([]TaskStatus, m.nMap)
	for i := 0; i < m.nMap; i++ {
		m.mapTasks[i].State = NotAssigned
	}
	m.reduceTasks = make([]TaskStatus, m.nReduce)
	for i := 0; i < m.nReduce; i++ {
		m.reduceTasks[i].State = NotAssigned
	}
	m.filesForMap = make([]string, m.nMap)
	m.filesForMap = files
	m.filesByReduce = make([]string, m.nReduce)
	m.server()
	go m.updateMasterStatus()
	go m.CheckWorkerAlive()
	return &m
}

func logDebugInfo(message string) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		fmt.Println("No caller information")
		return
	}

	// 使用 filepath.Base 获取文件的基础名称（不包含路径）
	fmt.Printf("[%s:%d] %s\n", filepath.Base(file), line, message)
}
