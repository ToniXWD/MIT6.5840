package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func GetRandomElectTimeOut() time.Duration {
	ms := rand.Int63() % 500
	return time.Duration(ElectTimeOutBase + ms)
}
