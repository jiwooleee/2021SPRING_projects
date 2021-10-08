package main

import (
	"flag"
	"time"
)

// User of the service.
type User struct {
	ID        int
	IsPremium bool
	TimeUsed  time.Duration
}

// HandleRequest runs the processes requested by users. Returns true iff the
// process completes.
func HandleRequest(process func(), u *User) bool {
	// TODO: time out and return false if process() is taking more than the user's
	// remaining time.

	res := make(chan bool)
	now := time.Now()
	go func() {
		process()
		res <- true
		u.TimeUsed += (time.Now().Sub(now))
	}()
	timer := time.NewTimer(10 * time.Second) // limit the processing time to 10 sec
	select {
	case <-res:
		return true
	case <-timer.C:
		return false
	}
}

func main() {
	flag.Parse()
	RunMockServer()
}
