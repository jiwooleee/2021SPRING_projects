# Exercise 1: Rate limiting 

# Limit your crawler

The [Tour of Go](https://tour.golang.org/) contains an exercise where you are to
write a web [crawler](https://tour.golang.org/concurrency/10).

This exercise contains a solution to that exercise. However, the solution is not
_rate limited_ -- it makes parallel requests at an unbounded rate. This sort of
behavior is considered abuse by many web servers, since web servers usually have
a maximum throughput at which the can serve requests, and many parallel requests
can overwhelm a web server. When done purposefully, this is called a [_denial of
service attack_](https://en.wikipedia.org/wiki/Denial-of-service_attack) (DoS).

In this exercise, you will use a rate limiting mechanism to make sure that no
more than one `Fetch` request happens per second.

You should __only modify code in `main.go`__. You can/should look at the other
files to understand the exercise, but only modify `main.go`.

## Hints

* This exercise can be solved by modifying only four lines (or three, depending
  on the approach).
* [This](https://github.com/golang/go/wiki/RateLimiting) page shows how to use
  `time.Tick` and `time.Ticker` to limit rates. See the Go documentation for the
  [time.Ticker](https://golang.org/pkg/time/#Ticker) or
  [time.Tick](https://golang.org/pkg/time/#Tick).
* By default, Go channels block when read from, so `<-ticker.C` will block until
  `ticker` ticks.

## Logging

These exercises will use [`glog`](https://github.com/golang/glog) logging module
for output, instead of printing.  This is a best-practice. Most systems run
without users observing their output, so logs are typically how program behavior
is observed.

Google logs defines four logging "levels:"

1. _Debug:_ Output events that are for debug purposes only, and will be turned
   off when the software is run in production.
1. _Info:_ Output important events that are not errors.
1. _Error:_ Output recoverable errors (_e.g._, a server being slow
   to respond, a URL that points to nowhere, etc.).
1. _Fatal:_ Events that prevent a program from continuing, and therefore will
   cause it to crash.

By default, all logs from the `glog` module are logged to files. When running
tests, only error logs are shown.

To see all logs, run with the `-logtostderr` flag.

Note that there are also different levels of debug logs, called _verbose logs_.
Verbose logs contain events that are so minor they are only useful when doing
low-level debugging. By default, no verbose logs are shown, even when debugging
logs are visible. `glog.V(2).Infof(...)` is a log statement at the verbosity
level of 2. This log statement will only be shown when run with the flag `-v 2`
or higher.

## Test your solution

Use `go test` to verify if your solution is correct.

Correct solution:
```
PASS
ok      concurrency-exercises/1-limiter 13.212s
```

Incorrect solution:
```
--- FAIL: TestMain (7.80s)
        main_test.go:18: There exists a two crawls who were executed less than 1 sec apart.
	        main_test.go:19: Solution is incorrect.
		FAIL
		exit status 1
		FAIL  concurrency-exercises/1-limiter    7.808s
```

## When you are complete

Make sure that you `git commit` and `git push` the branch you are working in.
Continue working in the same branch for the rest of the exercises.
