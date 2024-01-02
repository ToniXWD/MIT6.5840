package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// func GetRandomElectTimeOut(rd *rand.Rand) time.Duration {
// 	ms := int(rd.Float64() * 500.0)
// 	return time.Duration(ElectTimeOutBase + ms)
// }
