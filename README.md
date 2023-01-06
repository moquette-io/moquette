ServerIntegrationOpenSSLTest![Java CI with Maven](https://github.com/moquette-io/moquette/workflows/Java%20CI%20with%20Maven/badge.svg?branch=main)

## Moquette Project

[![Build Status](https://api.travis-ci.org/moquette-io/moquette.svg?branch=main)](https://travis-ci.org/moquette-io/moquette)

* [Documentation reference guide](http://moquette-io.github.io/moquette/) Guide on how to use and configure Moquette
* [Google Group](https://groups.google.com/forum/#!forum/moquette-mqtt) Google Group to participate in development discussions.

Moquette aims to be a MQTT compliant broker. The broker supports QoS 0, QoS 1 and QoS 2.

Its designed to be evented, uses Netty for the protocol encoding and decoding part.
 
## Embeddable

[Freedomotic](https://www.freedomotic-iot.com/) is an home automation framework and uses Moquette embedded to interface with MQTT by a specific [plugin](https://freedomotic-user-manual.readthedocs.io/en/latest/plugins/mqtt-broker.html). 

Moquette is also used into [Atomize Spin](http://atomizesoftware.com/spin) a software solution for the logistic field.

Part of moquette are used into the [Vertx MQTT module](https://github.com/giovibal/vertx-mqtt-broker-mod), into [MQTT spy](http://kamilfb.github.io/mqtt-spy/)
and into [WSO2 Messge broker](http://techexplosives-pamod.blogspot.it/2014/05/mqtt-transport-architecture-wso2-mb-3x.html).

## 1 minute set up

Start play with it, download the self distribution tar from [BinTray](https://bintray.com/artifact/download/andsel/generic/moquette-0.15.tar.gz) ,
the un untar and start the broker listening on `1883` port and enjoy!

```
tar xvf moquette-distribution-0.15.tar.gz
cd bin
./moquette.sh
```

Or if you are on Windows shell

```
 cd bin
 .\moquette.bat
```

## Embedding in other projects

Include dependency in your project: 

```
<dependency>
      <groupId>io.moquette</groupId>
      <artifactId>moquette-broker</artifactId>
      <version>0.15</version>
</dependency>
```

## Build from sources

After a git clone of the repository, cd into the cloned sources and: `./gradlew package`, at the end the distribution 
package is present at `distribution/target/distribution-0.17-SNAPSHOT-bundle.tar.gz`

In distribution/target directory will be produced the selfcontained file for the broker with all dependencies and a running script. 
