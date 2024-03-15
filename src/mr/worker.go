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
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doHeartBeat() *Task {
	t := Task{}
	call("Master.HeartBeat", struct{}{}, &t)
	if (t.TaskType == Map || t.TaskType == Reduce) && len(t.Files) == 0 {
		fmt.Println(t)
		panic("rpc通信得到一个有问题的Task")
	}
	return &t
}
func doReduce(reducef func(string, []string) string, t *Task) {
	var intermediateKV []KeyValue
	var kv KeyValue

	for _, file := range t.Files {
		f, err := os.Open(file)
		if err != nil {
			panic(err)
		}
		decoder := json.NewDecoder(f)
		for {
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(intermediateKV))
	oname := fmt.Sprintf("mr-out-%d", t.ID)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediateKV) {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateKV[k].Value)
		}
		output := reducef(intermediateKV[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediateKV[i].Key, output)
		i = j
	}
	ofile.Close()
	//fmt.Printf("%d reduce 任务完成,查看%s文件\n", t.ID, oname)
	execPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	reportTask := &Task{
		TaskType: t.TaskType,
		ID:       t.ID,
		Files:    []string{execPath + oname},
		NReduce:  t.NReduce,
	}
	//log.Println(reportTask.Files[0])
	submitTaskCompletionReport(reportTask)

}

func doMap(mapf func(string, string) []KeyValue, t *Task) {
	defer func() {
		if err := recover(); err != nil {
			panic(err)
		}
	}()
	if t.TaskType != Map {
		panic("do Map")
	}
	filename := t.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	// 一个map任务的产的键值对分成nReduce份
	buckets := make([][]KeyValue, t.NReduce)
	var index int
	for _, kv := range intermediate {
		index = ihash(kv.Key) % t.NReduce
		buckets[index] = append(buckets[index], kv)
	}
	for i := 0; i < t.NReduce; i++ {
		newfileName := fmt.Sprintf("mr-%d-%d", t.ID, i)
		tempfile, err := os.Create(newfileName)
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
		tempfile.Close()
	}
	execPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	var intermediatefiles []string
	for i := 0; i < t.NReduce; i++ {
		intermediatefiles = append(intermediatefiles, execPath+fmt.Sprintf("/mr-%d-%d", t.ID, i))
	}
	reportTask := &Task{
		TaskType: t.TaskType,
		ID:       t.ID,
		Files:    intermediatefiles,
		NReduce:  t.NReduce,
	}
	submitTaskCompletionReport(reportTask)
}

func submitTaskCompletionReport(t *Task) {
	call("Master.ReceiveTaskCompletionReport", t, &struct{}{})
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := doHeartBeat()
		//fmt.Println(task.TaskType, task.ID)
		switch task.TaskType {
		case Map:
			doMap(mapf, task)
		case Reduce:
			doReduce(reducef, task)
		case Wait:
			time.Sleep(2 * time.Second)
		case Over:
			return
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
