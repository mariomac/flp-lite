# FLP-lite

Lite version of [flowlogs-pipeline](https://github.com/netobserv/flowlogs-pipeline)
to research about potential refactorings and performance improvements:

Differences with [flowlogs-pipeline](https://github.com/netobserv/flowlogs-pipeline):

* No flexibility at all: it just implements the pipeline as required by the
  [NetObserv](https://github.com/netobserv) product.
* Instead of batching, processes elements 1 to 1. `chanBufLen` argument has been defined for
  each pipeline stage
* No Prometheus metrics report (**YET**)
* No Metrics generation (**YET**)
* Only Kafka ingestor supported, at the moment (flowlogs-pipeline-transformer)
* Only protobuf decoding supported (**YET**)
* Network transformer only supports add_kubernetes, add_service and add_subnet operations

## How to build

```
docker build -f build/Dockerfile --tag=quay.io/mmaciasl/flplite:main .
docker push quay.io/mmaciasl/flplite:main 
```