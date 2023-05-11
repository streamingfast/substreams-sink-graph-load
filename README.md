# Substreams to Subgraphs high-speed injector

This tool enables high-speed writes from a Substreams-based Subgraph and is 100% compatible with `graph-node`'s injection behavior (writing to postgres), including proofs of indexing.

It is an optional injection method that trades off high-speed injection for slightly more involved devops work.

High-speed here means one or two orders of magnitude faster.

## Requirement

* A Substreams package with a map module outputting `proto:substreams.entity.v1.EntityChanges`.
  * By convention, we use the module name `graph_out`
  * The [substreams-entity-change](https://github.com/streamingfast/substreams-entity-change) crate, contains the Rust objects and helpers to accelerate development of Substreams-based Subgraphs.

## Install

```bash
go install github.com/streamingfast/substreams-sink-graphcsv/cmd/substreams-sink-graphcsv@latest
```

## Use it to quickly fill your subgraph database

### Producing CSV from substreams data

| Note: to connect to substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|

1. Determine highest block that you want to index, aligned with your "bundle size" 
   If your bundle size is 10000 and the ethereum mainnet head block is at 17238813, your `STOP_BLOCK` will be 17230000.

2. Write the entities to disk, from substreams

```bash
substreams-sink-graphcsv run --chain-id=ethereum/mainnet --graphsql-schema=/path/to/schema.graphql --bundle-size=10000 /tmp/substreams-entities mainnet.eth.streamingfast.io:443 ./substreams-v0.0.1.spkg graph_out 17230000
```

3. Produce the CSV files based on an already-processed dump of entities:

```bash
for entity in $(cd /tmp/substreams-entities && ls); do 
    substreams-sink-graphcsv tocsv /tmp/susbtreams-entities /tmp/substreams-csv $entity 17230000 --bundle-size=10000 --graphql-schema=/path/to/schema.graphql
done
```

4. Verify that all CSV files were produced (from start-block rounded-down to bundle-size to the stop-block)

```bash
ls /tmp/substreams-csv/*
```

### Preparing your graph-node for injection

1. stop indexing on your node

```bash
graphman -c /etc/graph-node/config.toml unassign QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr
```

2. rewind to the block BEFORE your module initial block. (You can find the block hash on your favorite explorer).

```bash
# for a subgraph/substreams that start at block `12369620`
graphman -c /etc/graph-node/config.toml rewind 0x6a3bb2ef0a20f5503495238e54fef236659f56f1c57e1602b0de2b3d799fe154 12369620 QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr --force
```

### Injecting into postgres and restarting graph-node indexing:

1. Inject the csv files into postgres. List the files in `/tmp/substreams/csv/{entity}`

```bash
for entity in $(cd /tmp/substreams-csv && ls); do
    substreams-sink-graphcsv inject-csv QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr /tmp/substreams-csv $entity /path/to/schema.graphql 'postgresql://user:password@database.ip:5432/database' 12360000 17230000
done
```

2. Inform `graph-node` of the latest indexed block:

```bash
substreams-sink-graphcsv handoff QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr 0x0bdf3e2805450d917fbedb4d6f930d34261c3189eb14274e0b113302b28e59fe 17229999 'postgresql://user:password@database.ip:5432/database'
```

3. Restart `graph-node` indexing:

```bash
graphman -c /etc/graph-node/config.toml rewind 0x6a3bb2ef0a20f5503495238e54fef236659f56f1c57e1602b0de2b3d799fe154 12369620 QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr --force
```

## Postgresql indexes speedup

The `inject-csv` command can run even faster if the indexes have been dropped from postgresql. This is especially interesting for big datasets.

Here are a few hints about how to proceed:

* dropping indexes (before injection)

```
for entity in $(cd /tmp/substreams-csv && ls); do
    graphman -c /etc/graph-node/config.toml index list QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr $entity |grep -v -- '^-' > ${entity}.indexes
    for idx in $(awk '/^[a-z]/ {print $1}' ${entity}.indexes); do 
        graphman -c /etc/graph-node/config.toml index drop QmABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqr $idx
    done
done 
```

* creating indexes (after injection)

Edit the *.indexes so that this:

```
attr_3_25_pool_total_value_locked_usd_untracked using btree
  on (total_value_locked_usd_untracked)
```

becomes this: `create index attr_3_25_pool_total_value_locked_usd_untracked on sgd23.transaction using btree on (total_value_locked_usd_untracked)`

## LICENSE

Apache 2.0
