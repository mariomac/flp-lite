# FLP-lite

Differences with [flowlogs-pipeline](https://github.com/netobserv/flowlogs-pipeline):

* Instead of batching, processes elements 1 to 1. `chanBufLen` argument has been defined for
  each pipeline stage
* No Prometheus metrics report (**YET**)
* No Metrics generation (**YET**)
* Only Kafka ingestor supported, at the moment (flowlogs-pipeline-transformer)
* Only protobuf decoding supported (**YET**)
* Network transformer only supports add_kubernetes, add_service and add_subnet operations
