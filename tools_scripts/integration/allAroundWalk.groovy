@GrabResolver(name='Paho', root='https://repo.eclipse.org/content/repositories/paho-releases/')
@Grab(group='org.eclipse.paho', module='org.eclipse.paho.client.mqttv3', version='1.0.1', ext='jar')


import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

/**
 * A client that try all the simple MQTT commands
 * */

if (args.size() != 2) {
    println "usage: <host> <local client name>"
    return
}

String host = args[0]
String clientName = args[1]

String tmpDir = System.getProperty("java.io.tmpdir")
MqttDefaultFilePersistence dataStore1 = new MqttDefaultFilePersistence(tmpDir + "/" + clientName)
MqttClient client1 = new MqttClient("tcp://${host}:1883", "ConCli${clientName}", dataStore1)

println "start test"
print "connect..."
client1.connect()
println "OK!"

print "subscribe..."
client1.subscribe("/news", 0)
println "OK!"

print "unsubscribe..."
client1.unsubscribe("/news")
println "OK!"

print "disconnect..."
client1.disconnect()
println "OK!"
println "Done"
System.exit(0)
