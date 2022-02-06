package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "sync"
import "io/ioutil"
import "time"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker side:
// Spawn R threads, each thread starts ReqTaskRequest in an infinite loop
// ReqTaskResponse would tell worker if started a new Mapper/Reducer
// 1. for mapper, read certain file(assigned by ReqTaskResponse) and write to out put file(using key to locate the file)
// 2. for reducer, read certain intermediate file(according to task id) write to single output file mr-out-X.

// FinishTaskRequest - inform master some task has finished

var nReduce int

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetBucket(key string) int {
	return ihash(key) % nReduce
}

var mapfunc func(string, string) []KeyValue
var reducefunc func(string, []string) string

func ReadFromJsonFile(file_name string, kva *[]KeyValue, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatalf("cannot open %v", file_name)
	}
	dec := json.NewDecoder(file)
	var kv KeyValue
	for {
		if err := dec.Decode(&kv); err != nil {
			// log.Fatal(err)
			break
		}
		mu.Lock()
		*kva = append(*kva, kv)
		mu.Unlock()
	}
}

func ReduceWork(reducer *ReduceTask) {
	log.Printf("====ReduceWorker starts %v=====", reducer)
	idx := reducer.Idx
	var kva []KeyValue
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, file_name := range reducer.Im_file_names {
		wg.Add(1)
		ReadFromJsonFile(file_name, &kva, &wg, &mu)
	}
	wg.Wait()
	// Sanity check
	if len(kva) > 0 && GetBucket(kva[0].Key) != idx {
		log.Fatalf("Key is inconsistent with idx. key:%v, idx: %v", kva[0].Key, idx)
	}
	sort.Sort(ByKey(kva))
	output_file_name := fmt.Sprintf("mr-out-%d", idx)
	ofile, err := os.Create(output_file_name)
	if err != nil {
		log.Fatalf("Cannot create %v", output_file_name)
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducefunc(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	FinishReduceTask(reducer.Id, idx, output_file_name)
}

func MapWork(mapper *MapTask) {
	log.Printf("====MapWork starts %v=====", mapper)
	map_id := mapper.Id
	filename := mapper.File_name
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	im_file_names := make([]string, nReduce)
	tmp_files := make([]*os.File, nReduce)
	enc := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		im_file_names[i] = fmt.Sprintf("mr-%d-%d", map_id, i)
		tmpfile, err := ioutil.TempFile("", im_file_names[i])
		if err != nil {
			log.Fatal(err)
		}
		tmp_name := tmpfile.Name()
		// If any error occurs, remove the tmp_file. Normally the file would be renamed.
		defer os.Remove(tmp_name)

		tmp_files[i] = tmpfile
		enc[i] = json.NewEncoder(tmpfile)
	}

	for _, kv := range mapfunc(filename, string(content)) {
		bucket := GetBucket(kv.Key)
		if err := enc[bucket].Encode(&kv); err != nil {
			log.Fatalf("Cannot write json value %v into % v", kv, filename)
		}
	}

	for i, f := range tmp_files {
		os.Rename(f.Name(), im_file_names[i])
	}

	FinishMapTask(map_id, im_file_names)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapfunc = mapf
	reducefunc = reducef
	ReqNReduce()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// os.Remove("worker-log.txt")
	f, err := os.OpenFile("worker-log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	// Spawn nReduce threads to act as workers.
	for {
		reply := ReqTaskReply{}
		if !ReqTask(&reply) {
			log.Fatalf("Error in ReqTask")
		}
		log.Println("ReqTaskReply", reply)
		switch reply.Task_type {
		case 0:
			MapWork(&reply.Map_task)
		case 1:
			ReduceWork(&reply.Reduce_task)
		case 2:
			return
		case 3:
			time.Sleep(3 * time.Second)
		default:
			log.Fatalf("Error ReqTaskReply type: %v", reply.Task_type)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func ReqNReduce() {
	arg := ReqNReduceArgs{}

	reply := ReqNReduceReply{}

	call("Master.ReqNReduce", &arg, &reply)

	nReduce = reply.NReduce
	// log.Println("=======Init RPC done, nReduce:", nReduce, "=======")
}

func ReqTask(reply *ReqTaskReply) bool {
	args := ReqTaskArgs{}

	return call("Master.ReqTask", &args, &reply)
}

func FinishMapTask(id int, im_files []string) {
	args := FinishMapTaskArgs{
		Task_id:  id,
		Im_files: im_files,
	}
	log.Println("FinishMapTask", args)
	reply := FinishMapTaskReply{}
	call("Master.FinishMapTask", &args, &reply)
}

func FinishReduceTask(id int, idx int, output_file string) {
	args := FinishReduceTaskArgs{
		Task_id:          id,
		Idx:              idx,
		Output_file_name: output_file,
	}
	log.Println("FinishReduceTask", args)
	reply := FinishReduceTaskReply{}
	call("Master.FinishReduceTask", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Println("RPC call Error!!!:", err)
	return false
}
