package org.moquette.configurationmanager.examples;

import java.util.Map;

import org.moquette.configurationmanager.plugins.AbstractCustomPublishingNeeds;


public class GravityPublishingNeeds extends AbstractCustomPublishingNeeds{
	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return false;
	}

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload) {
		// TODO connect to gravity
		return super.doAdditinalWorkBeforePublishing(topic, user, clientID, payload);
	}

	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return false;
	}
}
