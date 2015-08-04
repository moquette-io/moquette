package org.moquette.configurationmanager.plugins;

import java.util.HashMap;
import java.util.Map;


public abstract class AbstractCustomPublishingNeeds implements ICustomPublishingNeeds{

	@Override
	public Map<String, String> doAdditinalWorkBeforePublishing(String topic,
			String user, String clientID, String payload) {
		Map<String, String> defaultValues = new HashMap<String,String>();
		defaultValues.put("topic", topic);
		defaultValues.put("user", user);
		defaultValues.put("clientID", clientID);
		defaultValues.put("payload", payload);
		return defaultValues;
	}
	
	
	
}
