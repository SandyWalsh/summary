# summary

## General Assumptions
* CSV files that don't parse are skipped
* CVS files with bad rows simply have the bad rows removed. 

## Design Assumptions
* This is being run as a batch process periodically. It's meant to output a file for downstream consumption. 
* We want to cap the number of goroutines that read the files to prevent taxing the system when the number of urls gets large. I'm hardcoding the cap at 3 for demo purposes but ideally it should be capped to somewhere around the number of cores on the system. 
* We can keep all the data in memory. If this was running on a small container we might have to break this up into smaller pieces, like a proper map-reduce solution. 

## Time constraints
Sorry, I was pressed for time on this challenge. As a result: 
* just log statements, no observability or telemetry support. 
* I took the `Ceil()` for the median not the average for an even number of entries. 
* I wrote this as a single module/main. It could be broken down to be much more readable with proper golang module use. 
* No tests currently. I know this is a sticky point. I can revisit if you need ... just might take me some time to get to it. 

## Discussion

* How would you change your program if it had to process many files where each file was over 10M records?
I wouldn't try to keep this in memory. I'd perhaps look at pumping each record into a queue and using something like Apache Beam/Dataflow to process the records in a streaming fashion. Also great for the sort operation required for median computation. 

* How would you change your program if it had to process data from more than 20K URLs?
Then we're into map-reduce territory. Memory constraints would be the big problem. As above, I would probably dump the urls into a queue and have a worker pool process them using a cloud native solution. Perhaps (in a GCP environment) something like Cloud Run that could trigger from the queue and process each URL with auto-scaling. The output could go in cloud storage for subsequent processing (like the Apache Beam solution I mentioned above for the 10M record problem). 

## To run
If you have golang (assuming 1.19) installed: 

```
go run main.go
```

Or if you prefer docker:

```
$ docker run $(docker build -q .)
```
or, depending on your installation:
```
$ sudo docker run $(sudo docker build -q .)
```