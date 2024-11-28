# Description

This is a sample REST API service implemented using franz-go to 
producing and consuming messages.


# Usage

Configure the kafka connection with environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_TOPICS=test-topic
KAFKA_GROUP=test-group
```

Run main.go:

```
go run main.go
```

## Produce sync

```shell
curl 'localhost:8080/sync' \
--header 'Content-Type: application/json' \
--data '{
    "id": 1
}'
```

## Produce Async

```shell
curl 'localhost:8080/async' \
--header 'Content-Type: application/json' \
--data '{
    "id": 10
}'
```

## Metrics

```shell
curl 'http://localhost:8080/metrics'
```

