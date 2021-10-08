// DO NOT EDIT THIS PART
// Your task is to edit `main.go`

package main

import (
	"github.com/golang/glog"

	"sync"
	"time"
)

var wg sync.WaitGroup

// RunMockServer pretends to be a processing service. It simulates user
// interacting with the Server.
func RunMockServer() {
	// Free user.
	u1 := User{ID: 0, IsPremium: false}
	// Paid user.
	u2 := User{ID: 1, IsPremium: true}

	wg.Add(5)

	go createMockRequest(1, shortProcess, &u1)
	time.Sleep(1 * time.Second)

	go createMockRequest(2, longProcess, &u2)
	time.Sleep(2 * time.Second)

	go createMockRequest(3, shortProcess, &u1)
	time.Sleep(1 * time.Second)

	go createMockRequest(4, longProcess, &u1)
	go createMockRequest(5, shortProcess, &u2)

	wg.Wait()
}

func createMockRequest(pid int, fn func(), u *User) {
	glog.Infof("User %d process %d started", u.ID, pid)
	res := HandleRequest(fn, u)

	var state string
	if res {
		state = "done"
	} else {
		state = "incomplete (no quota)"
	}
	glog.Infof("User %d process %d done, state %s.", u.ID, pid, state)

	wg.Done()
}

// short processes run for 4 seconds
func shortProcess() {
	time.Sleep(4 * time.Second)
}

// long processes run for 11 seconds
func longProcess() {
	time.Sleep(11 * time.Second)
}
