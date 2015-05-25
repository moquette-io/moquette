@Grab(group='org.fusesource.mqtt-client', module='mqtt-client', version='1.5')

import org.fusesource.mqtt.client.BlockingConnection
import org.fusesource.mqtt.client.MQTT
import org.fusesource.mqtt.client.Message
import org.fusesource.mqtt.client.QoS
import org.fusesource.mqtt.client.Topic


String host = args[0]
//start a publisher
MQTT mqtt2 = new MQTT()
mqtt2.setHost(host, 31883)
BlockingConnection publisher = mqtt2.blockingConnection()
publisher.connect()
println "publisher connected"

publisher.publish("/topic", 'Hello world again!!'.bytes, QoS.AT_MOST_ONCE, false)
println "publisher published"

println "shutdown publisher"
publisher.disconnect()