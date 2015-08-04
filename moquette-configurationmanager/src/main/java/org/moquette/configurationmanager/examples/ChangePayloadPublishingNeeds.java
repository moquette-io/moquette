package org.moquette.configurationmanager.examples;

import java.util.Map;

import org.moquette.configurationmanager.plugins.AbstractCustomPublishingNeeds;



public class ChangePayloadPublishingNeeds extends AbstractCustomPublishingNeeds{

	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return true;
	}

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload) {
		String newPayload = " + String sent from the plugin ... ";
		String finalPayload = payload + newPayload;
		return super.doAdditinalWorkBeforePublishing(topic, user, clientID, finalPayload);
	}

	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return true;
	}
}
