package org.moquette.configurationmanager.plugins;

import java.util.Map;

/**
 * Interface for any plugin that does its job after authorizing the user
 * 
 * @author williamkinaan
 *
 */

public interface ICustomPublishingNeeds {
	/**
	 * To know if there is additional job must be done before publishing the
	 * message
	 * 
	 * @param topic
	 * @param user
	 * @param clientID
	 * @return
	 */
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID);

	/**
	 * To do the additional job
	 * 
	 * @param topic
	 * @param user
	 * @param clientID
	 * @return
	 */
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload);

	/**
	 * To know if the message should be published aftering doing the aditional
	 * job. For instance, In my case, I don't want to publish them message after
	 * doing the additional job, which is connecting to my internal system, and
	 * create some events. One important thing, is that the broker will read the
	 * values of topic, user, and clientID from the output of this
	 * function(method) so please be sure that you support them. For example
	 * about how to add them, look at the DefaultPublishingNeeds , it is very
	 * simple
	 * 
	 * @param topic
	 * @param user
	 * @param clientID
	 * @return
	 */
	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID);
}
