package raft

import "log"

// Debugging
const Debug2A = true
const Debug2B = true
const Debug2C = false
const Debug2D = false

const Debugkill = false
const DebugAll = true

func DPrintf2A(format string, a ...interface{}) (n int, err error) {
	if Debug2A {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2B(format string, a ...interface{}) (n int, err error) {
	if Debug2B {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2C(format string, a ...interface{}) (n int, err error) {
	if Debug2C {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2D(format string, a ...interface{}) (n int, err error) {
	if Debug2D {
		log.Printf(format, a...)
	}
	return
}

func DPrintfKill(format string, a ...interface{}) (n int, err error) {
	if Debugkill {
		log.Printf(format, a...)
	}
	return
}

func DPrintfAll(format string, a ...interface{}) (n int, err error) {
	if DebugAll {
		log.Printf(format, a...)
	}
	return
}
