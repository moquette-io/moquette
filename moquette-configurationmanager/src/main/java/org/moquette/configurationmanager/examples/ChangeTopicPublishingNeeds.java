package org.moquette.configurationmanager.examples;

import java.util.Map;

import org.moquette.configurationmanager.plugins.AbstractCustomPublishingNeeds;

public class ChangeTopicPublishingNeeds extends AbstractCustomPublishingNeeds{
	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return true;
	}

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload) {
		topic = "NewTopic";
		return super.doAdditinalWorkBeforePublishing(topic, user, clientID, payload);
	}
	
	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return true;
	}
}
