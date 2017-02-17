# NIFI PubsubConsumer Processor

This is an [Apache NIFI](https://nifi.apache.org/) processor that reads messages from a Google Cloud Platform (GCP) PubSub topic. The operation is quite simple, it just needs to know the topic, subscription, priject ID authentication keys if it is running outside a GCP compute instance.

## Installation
* mvn pakage
* cp ./target/*.nar $NIFI_HOME/libs
