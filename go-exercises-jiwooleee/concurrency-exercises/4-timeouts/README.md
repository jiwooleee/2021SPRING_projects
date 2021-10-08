# Exercise 4: Resource accounting and timeouts

# Service level limiting

In this exercise, we will use goroutines with timeouts to model a scenario where
the service we are developing has two tiers of users: free and paid.

Free users will be limited to 10 seconds of processing time. After 10 seconds,
the service will ignore the process, and no further processes will be admitted.

In `main.go` you will find a `User` struct containing user metadata and
accumulated processing time. You can see in `HandleRequest` that time accounting
has been done for you.

You need to implement a timeout mechanism that makes `HandleRequest` return
false if the user ran out of processing time.

## Hints

* You should look at the time accounting code and look at the documentation for
  the [time](https://golang.org/pkg/time/) package.
* You will most likely use `time.Timer`/`time.NewTimer` from the `time` package
  to implement the timeout mechanism, along with `select`. You can see examples
  using select [here](https://tour.golang.org/concurrency/5), and guidance for
  how to implement a timeout [here](https://gobyexample.com/timeouts).

## Running the tests

Correct output:
```
go run . -logtostderr
I0119 20:19:02.779743   82468 mockserver.go:41] User 0 process 1 started
I0119 20:19:03.783207   82468 mockserver.go:41] User 1 process 2 started
I0119 20:19:05.783469   82468 mockserver.go:41] User 0 process 3 started
I0119 20:19:06.780226   82468 mockserver.go:50] User 0 process 1 done, state done.
I0119 20:19:06.784313   82468 mockserver.go:41] User 0 process 4 started
I0119 20:19:06.784335   82468 mockserver.go:41] User 1 process 5 started
I0119 20:19:09.788571   82468 mockserver.go:50] User 0 process 3 done, state done.
I0119 20:19:10.785848   82468 mockserver.go:50] User 1 process 5 done, state done.
I0119 20:19:12.784461   82468 mockserver.go:50] User 0 process 4 done, state incomplete (no quota).
I0119 20:19:14.786012   82468 mockserver.go:50] User 1 process 2 done, state done.
```
