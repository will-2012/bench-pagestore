# bench-pagestore

## Compile
```shell
make build
```

## Bench
```shell
# bench write
./bench/bench --write --write-qps 15000

# bench read
./bench/bench --read --read-qps 10000 --read-start 1 --read-end 15336898

# bench mix read/write
./bench/bench --mix --write-qps 10000 --read-qps 1000 --read-start 1 --read-end 15336898
```

## Monitor
```shell
curl http://127.0.0.1:6060/debug/metrics/prometheus
```