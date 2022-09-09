# summary

## Assumptions
* CSV files that don't parse are skipped
* CVS files with bad rows simply have the bad rows removed. 

## To run
If you have golang (assuming 1.19) installed: 
`go run main.go`

Or if you prefer docker:

```
$ docker run $(docker build -q .)
```
or, depending on your installation:
```
$ sudo docker run $(sudo docker build -q .)
```