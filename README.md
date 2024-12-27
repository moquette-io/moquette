[Java CI with Maven](https://github.com/moquette-io/moquette/workflows/Java%20CI%20with%20Maven/badge.svg?branch=main)

[![](https://jitpack.io/v/moquette-io/moquette.svg)](https://jitpack.io/#moquette-io/moquette)

## Moquette MQTT broker
[Documentation reference guide](http://moquette-io.github.io/moquette/) Guide on how to use and configure Moquette

Moquette is a lightweight broker compliant with MQTT 5 and MQTT 3, easily encapsulated in other applications.
The broker supports QoS 0, QoS 1 and QoS 2. The MQTT5 specification is almost fully supported. 
The features implemented by the broker are:
* session and message expiry
* shared subscriptions
* request/response
* topic alias
* flow control
* subscription options
* will delay
* server disconnects
* payload format indicator

Its designed to be evented, uses Netty for the protocol encoding and decoding part.

## Community feedback
We would love :heart: to hear from Moquette users, please [let us know how you use it 👣 ](https://github.com/moquette-io/moquette/discussions/874)

## Embedding in other projects

Use JitPack to resolve Moquette dependency in your project. 

In repositories section, add:
```
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>
```

In dependencies section add:
```
<dependency>
  <groupId>com.github.moquette-io</groupId>
  <artifactId>moquette-broker</artifactId>
  <version>0.18.0</version>
</dependency>
```

## Build from sources

After a git clone of the repository, cd into the cloned sources and: `./gradlew package`, at the end the distribution 
package is present at `distribution/target/distribution-0.19-SNAPSHOT-bundle.tar.gz`

In distribution/target directory will be produced the selfcontained file for the broker with all dependencies and a running script. 
