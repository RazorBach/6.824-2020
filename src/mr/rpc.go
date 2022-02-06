package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.
type MapTask struct {
	Id        int
	File_name string
}

type ReduceTask struct {
	Id            int
	Idx           int
	Im_file_names []string
}

type ReqNReduceArgs struct {
}

type ReqNReduceReply struct {
	NReduce int
}

type ReqTaskArgs struct {
}

type ReqTaskReply struct {
	Task_type   int // 0 - map; 1 - reduce; 2 - done
	Map_task    MapTask
	Reduce_task ReduceTask
}

type FinishMapTaskArgs struct {
	Task_id  int
	Im_files []string // Should be same len as nReduce
}

type FinishMapTaskReply struct {
}

type FinishReduceTaskArgs struct {
	Task_id          int
	Idx              int
	Output_file_name string // Should be same len as nReduce
}

type FinishReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
