package main

import (
	"flag"
	"sync"
	"time"

	"github.com/golang/glog"
)

var ticker *time.Ticker = time.NewTicker(time.Second)

// Crawl uses `fetcher` from the `mockfetcher.go` file to imitate a
// real crawler. It crawls until the maximum depth has reached.
func Crawl(url string, depth int, wg *sync.WaitGroup) {
	// Notify the WaitGroup upon completion.
	defer wg.Done()

	// Stop when we have reached maximum depth.
	if depth <= 0 {
		return
	}

	// TODO: Rate limit fetching.
	// Fetch and parse the URL.
	select {
	case <-ticker.C:
		body, urls, err := fetcher.Fetch(url)

		if err != nil {
			glog.Errorf("Error fetching %v: %v", url, err)
			return
		}

		glog.Infof("Found %s %q with %v urls", url, body, len(urls))

		wg.Add(len(urls))
		for _, u := range urls {
			glog.V(1).Infof("Crawling %s...", u)
			// Do not remove the `go` keyword, as Crawl() must be
			// called concurrently
			go Crawl(u, depth-1, wg)
		}
	}
	return
}

func main() {
	defer ticker.Stop()
	flag.Parse()

	var wg sync.WaitGroup

	wg.Add(1)
	Crawl("http://golang.org/", 4, &wg)
	wg.Wait()
}
