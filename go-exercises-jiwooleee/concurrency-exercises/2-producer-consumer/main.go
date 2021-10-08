package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

func producer(stream Stream, tweets chan<- *Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		tweet, err := stream.Next()
		if err == ErrEOF {
			close(tweets)
			return
		}
		tweets <- tweet
	}
}

func consumer(tweets <-chan *Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	for t := range tweets {
		if t.IsTalkingAboutRhodes() {
			fmt.Println(t.Username, "\ttweets about Rhodes")
		} else {
			fmt.Println(t.Username, "\tdoes not tweet about Rhodes")
		}
	}
}

func main() {
	flag.Parse()

	start := time.Now()
	stream := GetMockStream()

	// TODO: Allow producer and consumer to run in parallel.
	tweets := make(chan *Tweet)
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Producer
	go producer(stream, tweets, &wg)
	// Consumer
	go consumer(tweets, &wg)

	wg.Wait()
	fmt.Printf("Process took %s\n", time.Since(start))
}
