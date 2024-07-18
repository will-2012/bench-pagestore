# bench-pagestore

## Compile
```shell
make build
```

## Bench
```shell
# bench write
./bench/bench --write --write-qps 15000 --write-start 1

# bench read
./bench/bench --read --read-qps 3000 --read-start 1 --read-end 15336898

# bench notfound read
./bench/bench --read --notfound --read-qps 3000 --read-start 1 --read-end 15336898

# bench mix read/write
./bench/bench --mix --write-qps 3000 --write-start 15336898 --read-qps 1000 --read-start 1 --read-end 15336898
```

## Monitor
```shell
curl http://127.0.0.1:6060/debug/metrics/prometheus
```