package org.moquette.plugin.manager;

import org.moquette.plugin.manager.plugins.PluginConfigurationParser;
import org.moquette.plugin.manager.plugins.PluginTypes;

public class TestPluginParser {

	public static void main(String args[]) {
		new PluginConfigurationParser().parse();

		// PluginConfigurationParser.setPlugin(
		// PluginTypes.ICustomPublishingNeedsService,
		// DefaultPublishingNeeds.class);

		System.out.println(PluginConfigurationParser.plugins_configurations
				.getProperty(PluginTypes.ICustomPublishingNeedsService
						.toString()));
	}
}
