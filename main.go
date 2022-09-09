package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type user struct {
	first string
	last  string
	age   int
}

func (u user) String() string {
	return fmt.Sprintf("%s %s %d", u.first, u.last, u.age)
}

type payload struct {
	url      url.URL
	users    []user
	numBad   int
	err      error
	canRetry bool
	elapsed  time.Duration
}

func (p payload) String() string {
	if p.canRetry {
		return fmt.Sprintf("%s - retryable error - %s", p.url.String(), p.err)
	}
	if p.err != nil {
		return fmt.Sprintf("%s - non retryable error - %s", p.url.String(), p.err)
	}
	return fmt.Sprintf("%s %d users (%d skipped) elapsed:%s", p.url.String(), len(p.users), p.numBad, p.elapsed.String())
}

func parseCSV(data []byte) ([][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))

	csv, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	return csv, nil
}

// fetcher is the signature for a method that can read a file from a location.
// We can make different fetchers for different sources.
type fetcher func(url.URL) payload

// makePayload takes raw bytes, parses it into CSV, does some light validation, and returns a payload object.
func makePayload(url url.URL, b []byte) payload {
	p, err := parseCSV(b)
	if err != nil {
		return payload{url: url, err: err}
	}
	// do some basic validation. Start with the header
	if strings.Join(p[0], ",") != "fname, lname, age" {
		return payload{err: errors.New(fmt.Sprintf("%s does have proper CSV headers", url.String()))}
	}
	users := []user{}
	nbad := 0
	for _, r := range p[1:] { // clean up and check the rows
		for i, s := range r {
			r[i] = strings.TrimSpace(s)
		}
		// Assume each field has *some* data
		age, err := strconv.Atoi(r[2])
		if err != nil {
			// bad age ... skip row
			nbad += 1
			continue
		}
		u := user{first: r[0], last: r[1], age: age}
		if len(u.first) == 0 || len(u.last) == 0 || age == 0 {
			nbad += 1
			continue
		}
		users = append(users, u)
	}
	return payload{url: url, users: users, numBad: nbad}
}

// httpFetcher returns a payload from
func httpFetcher(url url.URL) payload {
	r, err := http.Get(url.String())
	if err != nil {
		return payload{url: url, err: err}
	}
	defer r.Body.Close()

	if r.StatusCode == http.StatusOK {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return payload{url: url, err: err}
		}
		return makePayload(url, body)
	}
	if r.StatusCode >= 500 {
		return payload{url: url, canRetry: true}
	}
	return payload{url: url, err: errors.New(fmt.Sprintf("unable to load file (status code %d)", r.StatusCode))}
}

func fileFetcher(url url.URL) payload {
	b, err := os.ReadFile(fmt.Sprintf("%s/%s", url.Host, url.Path))
	if err != nil {
		return payload{url: url, err: err}
	}
	return makePayload(url, b)
}

// urlFetcher accepts a url and will delegate to httpFetcher or fileFetcher depending on the url scheme
func urlFetcher(url url.URL) payload {
	switch url.Scheme {
	case "file":
		return fileFetcher(url)
	case "http":
		return httpFetcher(url)
	}
	return payload{url: url, err: errors.New(fmt.Sprintf("unknown url scheme: %s", url.Scheme))}
}

type results struct {
	payloads map[string]payload
	mtx      sync.Mutex
}

func (r *results) add(p payload, e time.Duration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	p.elapsed = e
	r.payloads[p.url.String()] = p
}

func worker(id int, todo chan url.URL, wg *sync.WaitGroup, f fetcher, r *results) {
	defer wg.Done()

	for url := range todo {
		log.Println("Worker ", id, "got", url.String())
		now := time.Now().UTC()
		p := f(url)
		elapsed := time.Since(now)
		r.add(p, elapsed)
	}
}

// fetch creates a goroutine pool and fetches all the csv files in batches.
// payloads with retryable errors are re-added to the queue
func fetch(urls []url.URL, f fetcher, poolSize int) ([]payload, time.Duration) {
	now := time.Now().UTC()

	ch := make(chan url.URL)
	var wg sync.WaitGroup

	// thread-safe place to put results
	r := &results{payloads: map[string]payload{}}

	// the initial remaining urls is the full list
	remaining := make([]url.URL, len(urls))
	copy(remaining, urls)

	var final []payload
	// feed urls into a constrained worker pool for processing
	for {
		for i := 0; i < poolSize; i++ { // cap worker pool size
			wg.Add(1)
			go worker(i, ch, &wg, f, r)
		}

		for _, url := range remaining {
			ch <- url
		}

		close(ch)
		wg.Wait()

		// Keep the payloads the successfully loaded, ignore the fatal errors, and retry what we can ...
		final = []payload{}
		remaining = []url.URL{}
		for _, v := range r.payloads {
			if v.err == nil {
				final = append(final, v)
			} else if v.canRetry {
				remaining = append(remaining, v.url)
			}
		}

		// more work to do?
		if len(remaining) == 0 {
			break
		} else {
			log.Println(len(remaining), "retryable urls left - cycling again")
		}
	}
	for _, v := range r.payloads {
		if v.err != nil {
			log.Println("skipping", v.url.String(), ":", v.err)
		}
	}

	return final, time.Since(now)
}

func loadIndex(index string) []url.URL {
	readFile, err := os.Open(index)
	defer readFile.Close()

	if err != nil {
		log.Fatal("unable to read csv index file:", err)
		os.Exit(1)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	files := []url.URL{}
	for fileScanner.Scan() {
		u := url.URL{Scheme: "file", Host: "data", Path: fileScanner.Text()}
		files = append(files, u)
	}
	return files
}

func merge(payloads []payload) []user {
	u := []user{}
	for _, p := range payloads {
		u = append(u, p.users...)
	}
	return u
}

func summarize(u []user) {
	log.Println(len(u), "users")
	if len(u) == 0 {
		return
	}
	n := len(u)
	ages := make([]int, n)
	var t int
	for i, x := range u {
		ages[i] = x.age
		t += x.age
	}
	sort.Ints(ages)
	mean := int(float64(t) / float64(n))
	log.Println("mean", mean)
	mid := int(math.Ceil(float64(n) / float64(2)))
	if mid < len(ages) {
		median := ages[mid]
		log.Println("median", median, "users:")
		for _, x := range u {
			if x.age == median {
				log.Println(x)
			}
		}
	}
}

func main() {
	files := loadIndex("index.txt")
	poolSize := 3
	payloads, elapsed := fetch(files, urlFetcher, poolSize)
	log.Println(len(payloads), "files read in", elapsed, "(poolsize", poolSize, ")")
	for _, p := range payloads {
		log.Println(p)
	}
	users := merge(payloads)
	summarize(users)
}
