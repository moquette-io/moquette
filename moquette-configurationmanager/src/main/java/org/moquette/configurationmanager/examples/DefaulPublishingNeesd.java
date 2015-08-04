package org.moquette.configurationmanager.examples;

import org.moquette.configurationmanager.plugins.AbstractCustomPublishingNeeds;

public class DefaulPublishingNeesd extends AbstractCustomPublishingNeeds {
	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return true;
	}


	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic, String user,
			String clientID) {
		return true;
	}

}
