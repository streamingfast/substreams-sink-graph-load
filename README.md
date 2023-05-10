# Substreams to Subgraphs high-speed injector

This tool enables high-speed writes from a Substreams-based Subgraph and is 100% compatible with `graph-node`'s injection behavior (writing to postgres), including proofs of indexing.

It is an optional injection method that trades off high-speed injection for slightly more involved devops work.

High-speed here means one or two orders of magnitude faster.

### Requirement

* A Substreams package with a map module outputting `proto:substreams.entity.v1.EntityChanges`.
  * By convention, we use the module name `graph_out`
  * The [substreams-entity-change](https://github.com/streamingfast/substreams-entity-change) crate, contains the Rust objects and helpers to accelerate development of Substreams-based Subgraphs.

### Install

```bash
go install github.com/streamingfast/substreams-sink-graphcsv/cmd/substreams-sink-graphcsv@latest
```

### Run

| Note: to connect to substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|

Process the Substreams and write entities to disk:
```bash
substreams-sink-graphcsv run --chain-id ethereum/mainnet --graphsql-schema ./path/to/schema.graphql /tmp/substreams-csv mainnet.eth.streamingfast.io:443 ./substreams-v0.0.1.spkg graph_out 100000
```

Produce the CSV files based on an already-processed dump of entities:

```bash
for i in $(ls out); do echo $i; substreams-sink-graphcsv tocsv $(pwd)/out $(pwd)/outcsv $i  12371850    --bundle-size=100 --graphql-schema=../substreams-uniswap-v3/schema.graphql    ; done
```

Inject into postgres:

```bash
substreams-sink-graphcsv inject [INSERT SAMPLE PARAMS]
```

Handoff to `graph-node`:

```bash
substreams-sink-graphcsv handoff [INSERT SAMPLE PARAMS]
```

### Dev commands

```bash
go install -v ./cmd/substreams-sink-graphcsv

time substreams-sink-graphcsv run $(pwd)/out api.streamingfast.io:443 ../substreams-uniswap-v3/substreams.yaml graph_out   12371895   --bundle-size=100 --graphql-schema=../substreams-uniswap-v3/schema.graphql

for i in $(ls out); do echo $i; substreams-sink-graphcsv tocsv $(pwd)/out $(pwd)/outcsv $i  12371850    --bundle-size=100 --graphql-schema=../substreams-uniswap-v3/schema.graphql    ; done
```

## LICENSE

Apache 2.0
