@GrabResolver(name='Paho', root='https://repo.eclipse.org/content/repositories/paho-releases/')
@Grab(group='org.eclipse.paho', module='org.eclipse.paho.client.mqttv3', version='1.0.1', ext='jar')


import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * A client that try all the simple MQTT commands
 * */

if (args.size() != 2) {
    println "usage: <host> <local client name>"
    return
}

class SubscriberCallback implements MqttCallback {

    private CountDownLatch m_latch = new CountDownLatch(1)

    void waitFinish() {
        boolean expired = m_latch.await(4, TimeUnit.SECONDS)
        if (expired) {
            println "Expired the time to receive the publish"
        }
    }

    void messageArrived(String topic, MqttMessage message) throws Exception {
        println "Received on [$topic] message: [$message]"
        m_latch.countDown()
    }

    void deliveryComplete(IMqttDeliveryToken token) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    void connectionLost(Throwable th) {
        println "Connection lost"
        m_latch.countDown()
    }
}


String host = args[0]
String clientName = args[1]

String tmpDir = System.getProperty("java.io.tmpdir")
MqttDefaultFilePersistence dataStore1 = new MqttDefaultFilePersistence(tmpDir + "/" + clientName)
MqttClient client1 = new MqttClient("tcp://${host}:1883", "ConCli${clientName}", dataStore1)
def callback = new SubscriberCallback()
client1.callback = callback

println "start test"
print "connect..."
client1.connect()
println "OK!"

print "subscribe..."
client1.subscribe("/news", 0)
println "OK!"

print "publish qos0..."
byte[] bytes = "Moquette is going to big refactoring".bytes
client1.publish("/news", bytes, 0, false)
callback.waitFinish()
println "OK!"

print "unsubscribe..."
client1.unsubscribe("/news")
println "OK!"

print "disconnect..."
client1.disconnect()
println "OK!"
println "Done"
System.exit(0)
