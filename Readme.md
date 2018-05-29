# Project Kolsch

This example will show how we can deploy a set of microservices and then reconfigure them on the fly using Apache Zookeeper.

# Running the demo

Start external servers. Tested with

* Kafka v2.11-1.1.0
* Zookeeper v3.4.10

I've just used the basic config from the Kafka Quickstart run both servers. They will be run on Openshift in the future.

Start the consumer with `mvn clean package exec:java`. 
Initially this will try to read the name of the topic to connect to from ZK://streams/app1/s1.
This node will not exist in ZK so the client will not connect to a topic.

Run the Zookeeper CLI (I don't think this comes bundled with Kafka so you'll need a separate download).

Create the nodes in Zookeeper to the names of the topics in Kafka. 

```bash
simon@MacBookPro:zookeeper-3.4.10 $ ./bin/zkCli.sh
Connecting to localhost:2181

...

[zk: localhost:2181(CONNECTED) 1] create /streams/app1/s1 test
Created /streams/app1/s1

```

You can send messages from the Kafka CLI to the `test` topic and these should be received by the consumer.
You should also see some logging on the consumer about which topic it is connected to (or not).
You can change the topic that the consumer is connected to by changing the value of the node

```bash
[zk: localhost:2181(CONNECTED) 4] set /streams/app1/s1 test2
```

You can delete the ZK node which will stop the consumer from receiving any messages.

```bash
[zk: localhost:2181(CONNECTED) 4] delete /streams/app1/s1 
```



# To Do

* Add lifecycle diagrams
* Producer and consumer APIs
* Test application to show reconfiguration
* How do the ZK keys that the application is using get wired up?
* How to inject into client code?
* How to deal with KStreams API?
* Openshiftify the demo

Using a `PathChildrenCache` as might in future be interested in multiple streams which could be represented as children of a common path.’


