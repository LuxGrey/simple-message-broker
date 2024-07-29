# Simple message broker

## Requirements

### General

* 3 programs for simple message broker architecture
  * communication between these is done using UDP
  * they are all non-interactive CLI programs
  * must fulfil their intended functions and be well documented
  * can be compiled using a standard C compiler (such as gcc)
* also an explanatory text that covers
  * how the programs function
  * the designed message broker protocol
  * the implementation of the 3 programs

### Publisher

* a request sent by the publisher consists auf a `topic` and a `message`
* `topic` and `message` are basic strings
* the publisher is called with the appropriate CLI arguments, sends a publish request to the broker and then terminates
* call pattern: `smbpuslish broker topic message`
  * `broker` = host name / IP-address of the broker
  * `topic` = the topic to publish to
  * `message` = the message to be published
* the publisher is not allowed to use the `#` wildcard for the topic

### Subscriber

* the subscriber is called with `topic` as a CLI argument, subscribes that topic at the specified broker and then listens for messages from the broker in an infinite loop, which it will then print to stdout
* call pattern: `smbsubscribe broker topic`
  * `broker` = host name / IP-address of the broker
  * `topic` = topic to subscribe to
* may use the `#` wildcard for `topic` to receive messages for all topics
  * make sure to place `#` in quotes when using it as a CLI argument, as it will otherwise be interpreted as a shell comment

### Broker

* allows subscribers to subscribe a topic, thus memorizing them for later message forwarding
* upon reception of a publish request by a publisher, immediately forwards the contained message to all subscribers that are currently subscribed to the respective topic
  * there is no need to retain received messages, they can be discarded as soon as they have been forwarded to the subscribers
* subscribers that subscribe to the `#` topic will receive messages from all topics

### Extra features

One may improve their grade for this project by adding further features on top
of the basic requirements that are defined above.
