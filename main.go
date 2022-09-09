package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	// performance data
}

func (p payload) String() string {
	if p.canRetry {
		return fmt.Sprintf("%s - retryable error - %s", p.url.String(), p.err)
	}
	if p.err != nil {
		return fmt.Sprintf("%s - non retryable error - %s", p.url.String(), p.err)
	}
	return fmt.Sprintf("%s %d users (%d skipped)", p.url.String(), len(p.users), p.numBad)
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

func makePayload(url url.URL, b []byte) payload {
	p, err := parseCSV(b)
	if err != nil {
		return payload{url: url, err: err}
	}
	// do some basic validation
	if strings.Join(p[0], ",") != "fname, lname, age" {
		return payload{err: errors.New(fmt.Sprintf("%s does have proper CSV headers", url.String()))}
	}
	users := []user{}
	nbad := 0
	for _, r := range p[1:] {
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
	return payload{url: url, canRetry: true}
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

// fetch creates a goroutine pool and fetches all the csv files in batches.
// payloads with retryable errors are re-added to the queue
func fetch(urls []url.URL, f fetcher) []payload {
	p := make([]payload, len(urls))
	p[0] = f(urls[0])
	return p
}

func summarize(p []payload) {
	log.Println("p[0]:", p[0])
	/*
		for _, u := range p[0].users {
			log.Println(u)
		}
	*/
}

func main() {
	readFile, err := os.Open("index.txt")
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

	fmt.Println(files)

	payloads := fetch(files, urlFetcher)
	summarize(payloads)
}
