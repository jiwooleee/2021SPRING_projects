# Exercise 2: Producer-Consumer

# Producer-Consumer model

The
[producer-consumer](https://www.cs.cornell.edu/courses/cs3110/2010fa/lectures/lec18.html)
model is a concurrency pattern that is frequently used in both concurrent and
distributed systems.

In the model, there are processes/threads that are denoted as _producers_.
Producers are generating output that should be consumed by _consumer_
processes/threads.

Examples of this model include things like iterrupts and interrupt handlers,
work items and workers in data processing systems like MapReduce (to be
discussed later this semester), etc.

In this exercise, we will take a sequential (_i.e._, not concurrent)
program with a producer and consumer and make it concurrent.

Our scenario will be this:

* A producer reads from the Twitter API (modeled here as a Go struct called
  `stream`) and emits tweets.
* A consumer reads from the emitted tweets and filters based on some criteria
  (does the tweet mention Rhodes).

Since reading from the Twitter API takes time (simulated by calls to
`time.Sleep` from `Next()` in `mockstream.go`), it should be possible to process
tweets while new tweets are being read from the API. However, the current
implementation is sequential.

Your job is to make the producer and consumer run _concurrently_ and therefore
increase the throughput of the program.

__You should only modify `main.go`__.

## Hints

* You will need to use a channel so that the consumer can read tweets as they
  are emitted. This means that you will have to create a channel and pass it to
  both the producer and consumer. This also means that the `producer` will not
  return a slice of tweets.
* A channel-based [for loop](https://tour.golang.org/concurrency/4) can be used
  in the consumer to read tweets as they become available, and then stop when
  the tweets are consumed.
* The producer and consumer should be goroutines that are started by `main.go`.
* Note that your `main` must wait for your consumer to be done consuming before
  exiting. You can use can either use a
  [WaitGroup](https://gobyexample.com/waitgroups) or a
  [channel](https://gobyexample.com/channel-synchronization) to be notified
  when the consumer is done.

## Expected results:

Before: 
```
go run .
PresidentHass   tweets about Rhodes
realsewanee     does not tweet about Rhodes
PJOfficial      tweets about Rhodes
PJOfficial      does not tweet about Rhodes
realsewanee     tweets about Rhodes
Process took 3.6074689s
```

After:
```
go run .
PresidentHass   tweets about Rhodes
realsewanee     does not tweet about Rhodes
PJOfficial      tweets about Rhodes
PJOfficial      does not tweet about Rhodes
realsewanee     tweets about Rhodes
Process took 1.984115072s
```
