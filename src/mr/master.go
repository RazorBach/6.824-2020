package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"

// import "io/ioutil"
import "time"

type Master struct {
	// Your definitions here.
	map_done    bool
	reduce_done bool

	task_id      int
	mapper_tasks map[int]MapTask
	mapper_ch    chan int

	im_idxs       []int
	reducer_tasks map[int]ReduceTask
	reducer_ch    chan int

	input_files []string
	im_files    [][]string
	mu          sync.Mutex
	nReduce     int
}

// Master side:
// Assign task when receiving ReqTaskRequest
// DOne->check R reducers are all done
// Read all files.
// Assign each incoming task with next unfinished file (mapper++)
// When no file - block until all mappers finished
// When all mappers finished: Assign each incoming task with next unfinished intermediate file
// When all reducers finished, done.

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) ReqNReduce(args *ReqNReduceArgs, reply *ReqNReduceReply) error {
	log.Println("=======Init RPC done, nReduce:", m.nReduce, "=======")
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	// 0 - assign mapper 1 - assign reducer 2 - reducer done 3 continue request
	m.mu.Lock()
	defer m.mu.Unlock()
	// log.Println("ReqTask from", args.Id, " Master ", m)
	if m.reduce_done {
		reply.Task_type = 2
		log.Println("Master jobs done")
		return nil
	} else if len(m.input_files) > 0 {
		if m.AssignMapTaskLocked(reply) {
			return nil
		}
		// Differentiate between remaing reduce tasks and done
	} else if m.map_done && len(m.im_idxs) > 0 {
		if m.AssignReduceTaskLocked(reply) {
			return nil
		}
	}
	reply.Task_type = 3
	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("FinishMapTask from ", args)
	mapper_id := args.Task_id
	if mapper, ok := m.mapper_tasks[mapper_id]; !ok {
		return fmt.Errorf("Unassigned mapper_id: %v!", mapper_id)
	} else {
		for i, im_file := range args.Im_files {
			if len(im_file) > 0 {
				m.im_files[i] = append(m.im_files[i], im_file)
			}
		}
		delete(m.mapper_tasks, mapper_id)
		if len(m.input_files) == 0 && len(m.mapper_tasks) == 0 {
			m.map_done = true
		}
		log.Println("Mapper done ", mapper, " im files: ", args.Im_files)
	}
	return nil
}

func (m *Master) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("FinishReduceTask from ", args)
	reducer_id := args.Task_id
	if reducer, ok := m.reducer_tasks[reducer_id]; !ok {
		return fmt.Errorf("Unassigned reducer_id: %v!", reducer_id)
	} else {
		delete(m.reducer_tasks, reducer_id)
		if len(m.im_idxs) == 0 && len(m.reducer_tasks) == 0 {
			m.reduce_done = true
		}
		for _, im_file_name := range reducer.Im_file_names {
			os.Remove(im_file_name)
		}
		log.Println("Reducer done ", args)
	}
	return nil
}

func (m *Master) FindNextSplitFileLocked(file_name *string) bool {
	if len(m.input_files) == 0 {
		return false
	}
	*file_name = m.input_files[0]
	m.input_files = m.input_files[1:]
	return true
}

func (m *Master) GenerateTaskIdLocked() int {
	ret := m.task_id
	m.task_id += 1
	return ret
}

func (m *Master) CheckMapperTaskStatus() {
	for id := range m.mapper_ch {
		go func(mapper_id int) {
			time.Sleep(10 * time.Second)
			log.Println("Check Mapper Task ", mapper_id)
			m.mu.Lock()
			defer m.mu.Unlock()
			if mapper, ok := m.mapper_tasks[mapper_id]; ok {
				log.Println("Mapper not finished for ", mapper, " in 10 seconds")
				delete(m.mapper_tasks, mapper_id)
				m.input_files = append(m.input_files, mapper.File_name)
			}
		}(id)
	}
}

func (m *Master) CheckReducerTaskStatus() {
	for id := range m.reducer_ch {
		go func(reducer_id int) {
			time.Sleep(10 * time.Second)
			log.Println("Check Reducer Task ", reducer_id)
			m.mu.Lock()
			defer m.mu.Unlock()
			if reducer, ok := m.reducer_tasks[reducer_id]; ok {
				log.Println("Reducer not finished for ", reducer, " in 10 seconds")
				delete(m.reducer_tasks, reducer_id)
				m.im_idxs = append(m.im_idxs, reducer.Idx)
			}
		}(id)
	}
}

// Assign with next unfinished file, wait for 10 seconds to check if the same task id exists
// If exists, move the file_name back to input files for reassign map task
func (m *Master) AssignMapTaskLocked(reply *ReqTaskReply) bool {
	var file_name string
	var mapper_id int
	// log.Println("AssignMapTaskLocked starts")
	if !m.FindNextSplitFileLocked(&file_name) {
		log.Println("Error! AssignMapTaskLocked failed, missing unfinished input files.")
		return false
	}
	// log.Println("FindNextSplitFileLocked", file_name)
	mapper_id = m.GenerateTaskIdLocked()
	reply.Task_type = 0
	map_task := MapTask{
		Id:        mapper_id,
		File_name: file_name,
	}
	reply.Map_task = map_task
	m.mapper_tasks[mapper_id] = map_task
	log.Println(reply)
	m.mapper_ch <- mapper_id
	return true
}

func (m *Master) AssignReduceTaskLocked(reply *ReqTaskReply) bool {
	reply.Task_type = 1
	// log.Println("AssignReduceTaskLocked")
	if len(m.im_idxs) == 0 {
		log.Println("Error! AssignReduceTaskLocked failed, missing unfinished reduce id.")
		return false
	}
	reducer_id := m.GenerateTaskIdLocked()
	im_idx := m.im_idxs[0]
	m.im_idxs = m.im_idxs[1:]
	reduce_task := ReduceTask{
		Id:            reducer_id,
		Idx:           im_idx,
		Im_file_names: m.im_files[im_idx],
	}
	reply.Reduce_task = reduce_task
	m.reducer_tasks[reducer_id] = reduce_task
	m.reducer_ch <- reducer_id
	go func(reducer_id int) {
		time.Sleep(10 * time.Second)
	}(reducer_id)
	return true
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.reduce_done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:       nReduce,
		map_done:      false,
		reduce_done:   false,
		input_files:   files,
		task_id:       0,
		mapper_tasks:  make(map[int]MapTask),
		reducer_tasks: make(map[int]ReduceTask),
		im_files:      make([][]string, nReduce),
		mapper_ch: 	   make(chan int, 10),
		reducer_ch:    make(chan int, 10),
	}
	for i := 0; i < nReduce; i++ {
		m.im_idxs = append(m.im_idxs, i)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Remove("master-log.txt")
	f, err := os.OpenFile("master-log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	// defer f.Close()
	log.SetOutput(f)

	// log.SetOutput(ioutil.Discard)
	m.server()
	go m.CheckMapperTaskStatus()
	go m.CheckReducerTaskStatus()
	return &m
}
