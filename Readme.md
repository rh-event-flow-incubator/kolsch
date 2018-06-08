# Project Kolsch

This example will show how we can deploy a set of microservices and then reconfigure them on the fly using Apache Zookeeper.
The microservices are implemented as Fat Jars which run a 'Wrapper' class to do the configuration of the actual producer / consumer.

The intention is that the services can be deployed first and will wait for the conifuguration to appear later.
The services are configured with environment variables which specify the connection to Zookeeper and a path to watch.
When the path that is being watched has certain children (`kafkaUrl` and `topic`) the service thread will be started.
If the values of these znodes change the wrapper is notified using Apache Curator framework. 
This results in the service thread being terminated and another one started with updated paramters.

At the moment only the Kafka connection settings and topic to communicate with can be specified. 
A later version will allow other parameters to be specified such as Serdes.

# Modules

* zkWrapper - implementation of a single and multi-topic wrapper. 
  Multi-topic wrapper is used for Kafka Streams or other applications that need to communicate with more than one stream.
* Producer - simple producer that will send and receive messages.
* Consumer - consumes messages and logs them.
* Processor - Uses KStreams API to split messages into one word per message. 
* Demo - Versions of the demo. One for Openshift and one standalone that requires Zookeeper and Kafka to be running locally.

# Running the Example

Deploy [Strimzi](http://strimzi.io/) into Openshift.

Deploy the microservices to Openshift `mvn clean package fabric8:deploy` from the producer, processor and consumer directories.
Once deployed you should see the following log messages in the pod log indicating the service has started but is awaiting configuration.

```bash
Jun 06, 2018 10:15:43 AM com.redhat.streaming.zk.wrapper.ZKSingleTopicWrapper run
INFO: Not connected to topic
```
The demo uses the Zookeeper instance that is used internally by Strimzi (my-cluster-zookeeper) but a separate one could be provisioned.
This will be changed in future as the security around Zookeeper indicates it would be better limited to a single application. 
There are two ways to wire up the example: the easy way and the hard way.

## The Easy Way

The demo project is setup to wire the three microservices Producer -> Processor -> Consumer. 
Deploy this project into Openshift and the services will reconifugure themselves and start working.
If you change the names of the topics that are specified the wiring of the application will change.


## The Hard Way
 
Use the online terminal of the Zookeeper pod to issue the following commands:

```bash
> cd bin

# Start the consumer
> ./zookeeper-shell.sh localhost:2181 <<< "create /streams/consumer/kafkaUrl my-cluster-kafka:9092"
> ./zookeeper-shell.sh localhost:2181 <<< "create /streams/consumer/topic topic1"

# Start the producer
>./zookeeper-shell.sh localhost:2181 <<< "create /streams/producer/kafkaUrl my-cluster-kafka:9092"
> ./zookeeper-shell.sh localhost:2181 <<< "create /streams/producer/topic topic1"

```
If you now look at the console of the producer and consumer you will see messages being sent and received. 
To reconfigure the 'wiring' change the values of the znodes for the topics. 
For instance `> ./zookeeper-shell.sh localhost:2181 <<< "set /streams/consumer/topic topic2"` will restart the consumer listening to `topic2`.
Similar commands can be applied to the producer in order to wire it back up again.

# Lifecycle

The implementation that uses fat jars deployed into Openshift. 
This means that the wrapper can control the lifecycle of the thread doing the work and restart it when the znodes have changed.
This technique will not work with Thorntail but it is possible that the classes could be hot swapped when a change is detected using [Hotswap Agent](http://hotswapagent.org/). 

# To Do

* Add diagrams
* Add support for Thorntail and other RHOARs.
* Inject wrapper into client code. Currently the developer has to manually setup the wrapper. It should be injected.
* The wiring that is done using the Zookeeper shell should be done via Config Maps. 
  An operator component will be developed which will watch for Config Map updates and apply the changes to Zookeeper. 


