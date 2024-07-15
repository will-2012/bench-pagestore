# bench-pagestore

## Compile
```shell
make build
```

## Bench
```shell
# bench write
./bench/bench --write

# bench read
./bench/bench --read --read-start 10 --read-end 100
```

## Monitor
```shell
curl http://127.0.0.1:6060/debug/metrics/prometheus
```