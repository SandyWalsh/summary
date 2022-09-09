# syntax=docker/dockerfile:1

FROM golang:1.19

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY *.go ./
COPY data ./data
COPY index.txt ./

RUN go build -o /summary

CMD [ "/summary" ]