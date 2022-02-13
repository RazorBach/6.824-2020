package main

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "../mr"
import "strings"


//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return r == '\n' }

	pattern := "now"

	kva := []mr.KeyValue{}
	for _, line := range strings.FieldsFunc(contents, ff) {
		if strings.Contains(line, pattern) {
			kv := mr.KeyValue{filename, line}
			kva = append(kva, kv)
		}
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strings.Join(values, "\n")
}