package mapreduce

import (
	"fmt"
	"sync"
)

type callRet struct {
	worker string
	flag   bool
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	workerState := make(map[string]bool)
	ch := make(chan callRet)
	wg := &sync.WaitGroup{}
	wg.Add(ntasks)
	index := 0
	f := func(w string, index int) {
		args := DoTaskArgs{jobName, mapFiles[index], phase, index, n_other}
		res := call(w, "Worker.DoTask", args, nil)
		ch <- callRet{w, res}
	}

	cnt := 0
loop:
	for {
		select {
		case w := <-registerChan:
			if index < ntasks {
				workerState[w] = true
				go f(w, index)
				index++
			}
		case ret := <-ch:
			// if failed
			if !ret.flag {
				index--
				for k, v := range workerState {
					if !v {
						go f(k, index)
						index++
						break
					}
				}
				break
			}
			workerState[ret.worker] = false
			wg.Done()
			cnt++

			if cnt >= ntasks {
				break loop
			}
			if index < ntasks {
				go f(ret.worker, index)
				index++
			}
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
