@GrabResolver(name='moquette-bintray', root='http://dl.bintray.com/andsel/maven/')
@Grab(group='org.dna.mqtt', module='moquette-broker', version='0.6')

import org.eclipse.moquette.server.Server

println "Starting broker in embedded mode"
Server server = new Server()
//Properties props = [port: 31883, host: "0.0.0.0", 'password_file': '../broker/config/password_file.conf'] as Properties
Properties props = new Properties()
props.setProperty('port', '31883')
props.setProperty('host', "0.0.0.0")
props.setProperty('password_file', '../broker/config/password_file.conf')
println "starting proprs $props"
server.startServer(props)
println "Stopping broker.."
server.stopServer()