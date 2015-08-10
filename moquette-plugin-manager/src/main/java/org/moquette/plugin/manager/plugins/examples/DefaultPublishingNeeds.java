package org.moquette.plugin.manager.plugins.examples;

import org.moquette.plugin.manager.plugins.custompublishingneeds.AbstractCustomPublishingNeeds;

public class DefaultPublishingNeeds extends AbstractCustomPublishingNeeds {

	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return false;
	}

	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return true;
	}

}
