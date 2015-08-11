package org.moquette.plugin.manager;

import org.moquette.plugin.manager.plugins.PluginConfigurationParser;
import org.moquette.plugin.manager.plugins.PluginTypes;
import org.moquette.plugin.manager.plugins.examples.ChangeTopicPublishingNeeds;

public class TestPluginParser {

	public static void main(String args[]) {
		PluginConfigurationParser.parse();

		// PluginConfigurationParser.setPlugin(
		// PluginTypes.ICustomPublishingNeedsService,
		// DefaultPublishingNeeds.class);

		PluginConfigurationParser.setPlugin(
				PluginTypes.ICustomPublishingNeedsService,
				ChangeTopicPublishingNeeds.class);

		// server.getPluginManager().setAuthentiction

		System.out.println(PluginConfigurationParser.plugins_configurations
				.getProperty(PluginTypes.ICustomPublishingNeedsService
						.toString()));
	}
}
