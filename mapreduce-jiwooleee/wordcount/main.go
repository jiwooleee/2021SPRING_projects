package main

import (
	"flag"
	"fmt"
	"mapreduce/mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/golang/glog"
)

var (
	worker      = flag.Bool("worker", false, "Whether to run as a worker.")
	managerAddr = flag.String("managerAddr", "localhost:8888", "Address of manager.")
	workerAddr  = flag.String("workerAddr", "localhost:7777", "Address of worker.")
	distributed = flag.Bool("distributed", false, "Whether to run distributed.")
	rShards     = flag.Int("rShards", 3, "Number of reduce shards.")
)

// The Map function.
func mapFn(filename string, contents string) []mapreduce.KeyValue {
	// key == word, value == wordcount
	keyvalue := make(map[string]int)
	// slice
	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	input := strings.FieldsFunc(contents, f)

	for _, key := range input {
		value, okay := keyvalue[key]
		// key NOT in keyvalues
		if !okay {
			keyvalue[key] = 1
		} else {
			// key in keyvalues
			keyvalue[key] = value + 1
		}
	}
	// write keyvalue to []keyvalue mapOutput
	mapOutput := make([]mapreduce.KeyValue, 0, 9999)
	for key := range keyvalue {
		mapOutput = append(mapOutput, mapreduce.KeyValue{
			Key:   key,
			Value: strconv.Itoa(keyvalue[key]),
		})
	}
	return mapOutput
}

// The Reduce function.
func reduceFn(key string, values []string) string {
	wordcount := 0
	for _, v := range values {
		value, err := strconv.Atoi(v)
		if err != nil {
			glog.Fatalln("Fatal: ", err)
		}
		wordcount += value
	}
	return strconv.Itoa(wordcount)
}

// wordcount can be run in 3 ways:
// 1) Sequential: go run mapreduce/mapreduce x1.txt .. xN.txt
// 2) Manager: go run mapreduce/mapreduce -distributed x1.txt .. xN.txt
// 3) Worker: go run mapreduce/mapreduce -worker
func main() {
	flag.Parse()

	if len(flag.Args()) < 1 && !*worker {
		fmt.Println("See usage in README.md: must supply input files to read",
			" if not a worker.")
		os.Exit(1)
	}

	if *distributed && !*worker && *managerAddr == "" {
		fmt.Println("Must supply a managerAddr if running as manager.")
		os.Exit(1)
	}

	if *distributed && *worker && *managerAddr == "" {
		fmt.Println("Must supply a managerAddr for a worker.")
		os.Exit(1)
	}

	if *distributed && *worker && *workerAddr == "" {
		fmt.Println("Must supply a workerAddr if running as worker.")
		os.Exit(1)
	}

	if !*worker {
		fmt.Println("Running MapReduce...")
		spec := mapreduce.MapReduceSpec{
			Files:    flag.Args(),
			R:        *rShards,
			MapFn:    mapFn,
			ReduceFn: reduceFn}
		// Run Manager
		var mr *mapreduce.Manager
		if *distributed {
			// Run distributed MR.
			spec.JobName = "wordcount-parallel"
			mr = mapreduce.MapReduce(*managerAddr, spec)
		} else {
			// Run sequential MR.
			spec.JobName = "wordcount-sequential"
			mr = mapreduce.LocalMapReduce(spec)
		}
		mr.Wait()
		fmt.Println("Done!")
	} else {
		// Run Worker
		spec := mapreduce.WorkerConfig{
			MapFn:    mapFn,
			ReduceFn: reduceFn}
		mapreduce.RunWorker(*managerAddr, *workerAddr, spec, -1, nil)
	}
}
