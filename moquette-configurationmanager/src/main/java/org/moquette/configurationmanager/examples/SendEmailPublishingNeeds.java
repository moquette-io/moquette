package org.moquette.configurationmanager.examples;

import java.util.Map;

import org.moquette.configurationmanager.plugins.AbstractCustomPublishingNeeds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendEmailPublishingNeeds extends AbstractCustomPublishingNeeds{

	private static final Logger LOG = LoggerFactory
			.getLogger(SendEmailPublishingNeeds.class);
	
	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return true;
	}

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String msg) {
		
		LOG.info("------------------");
		LOG.info("Sending and email to someone...");
		// TODO Send an email
		LOG.info("Done, sent successfully !");
		LOG.info("------------------");
		return super.doAdditinalWorkBeforePublishing(topic, user, clientID, msg);
	}

	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return true;
	}

}
