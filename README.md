# Substreams graph-node entity CSV writer

This is a command line tool to quickly sync a substream to CSV files that can then be imported directly into the postgresql database of a subgraph

### Running It

1) Your substreams needs to implement a `map` that has an output type of `proto:substreams.entity.v1.EntityChanges`.
   By convention, we name the `map` module `graph_out`. The [substreams-entity-change](https://github.com/streamingfast/substreams-entity-change) crate, contains the rust objects.


2) Run the sink

| Note: to connect to substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|

Usage:

```shell
Runs substreams sinker to CSV files

Usage:
  substreams-sink-graphcsv run <destination-folder> <endpoint> <manifest> <module> <stop> [flags]

Flags:
  -h, --help        help for run
  -k, --insecure    Skip certificate validation on GRPC connection
  -p, --plaintext   Establish GRPC connection in plaintext

Global Flags:
      --delay-before-start duration   [OPERATOR] Amount of time to wait before starting any internal processes, can be used to perform to maintenance on the pod before actually letting it starts
      --metrics-listen-addr string    [OPERATOR] If non-empty, the process will listen on this address for Prometheus metrics request(s) (default "localhost:9102")
      --pprof-listen-addr string      [OPERATOR] If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/) (default "localhost:6060")
```

Example:

```shell
go install ./cmd/substreams-sink-graphcsv
substreams-sink-graphcsv run \
/tmp/substreams-csv \
mainnet.eth.streamingfast.io:443 \
./substreams-v0.0.1.spkg \
graph_out \
100000
```

Development commands:
```
go install -v ./cmd/substreams-sink-graphcsv

time substreams-sink-graphcsv run $(pwd)/out api.streamingfast.io:443 ../substreams-uniswap-v3/substreams.yaml graph_out   12371895   --bundle-size=100 --graphql-schema=../substreams-uniswap-v3/schema.graphql


for i in $(ls out); do echo $i; substreams-sink-graphcsv tocsv $(pwd)/out $(pwd)/outcsv $i  12371850    --bundle-size=100 --graphql-schema=../substreams-uniswap-v3/schema.graphql    ; done

```