/**
 * 
 */
package org.moquette.plugin.manager.plugins.custompublishingneeds;

import java.util.Map;

import org.moquette.plugin.manager.plugins.IPlugin;

/**
 * @author williamkinaan
 *
 */
public interface ICustomPublishingNeeds extends IPlugin {
	public boolean isThereAditionalChecksBeforePublishing(String topic,
			String user, String clientID);

	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload);

	public boolean shouldPublishTheMessageToAllClients(String topic,
			String user, String clientID);
}
