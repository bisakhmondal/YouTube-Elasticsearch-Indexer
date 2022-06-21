##
## Build
##
FROM golang:1.17-alpine3.16 AS build

WORKDIR /app

COPY . /app/
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -o indexer .

##
## Deploy
##
FROM alpine:3.16

WORKDIR /deploy

COPY --from=build /app/indexer .
COPY --from=build /app/config.json .

EXPOSE 8880

ENTRYPOINT ["./indexer", "--config", "config.json"]
