ps aux|grep org.eclipse.moquette.server.Server|awk '{print $2}'|xargs kill -9
