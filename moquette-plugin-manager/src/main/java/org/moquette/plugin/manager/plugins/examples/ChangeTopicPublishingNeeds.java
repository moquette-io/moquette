package org.moquette.plugin.manager.plugins.examples;

import java.util.Map;

import org.moquette.plugin.manager.plugins.custompublishingneeds.AbstractCustomPublishingNeeds;

/**
 * This is a custom plugin to change the topic that the users send to The users
 * will send to any topic, but the messages will be re directed to a new topic
 * called "NewTopic"
 * 
 * @author williamkinaan
 *
 */
public class ChangeTopicPublishingNeeds extends AbstractCustomPublishingNeeds {
	@Override
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID) {
		return true;
	}

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload) {
		topic = "NewTopic";
		return super.doAdditinalWorkBeforePublishing(topic, user, clientID,
				payload);
	}

	@Override
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID) {
		return true;
	}
}
