package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("raft-"+format, a...)
	}
	return
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 150)

	return plusMs + ElectTimeOutBase
}
