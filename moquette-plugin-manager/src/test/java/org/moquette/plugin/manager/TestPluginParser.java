package org.moquette.plugin.manager;

import org.moquette.plugin.manager.plugins.PluginConfigurationParser;

public class TestPluginParser {

	public static void main(String args[]) {
		new PluginConfigurationParser().parse();
		System.out.println(PluginConfigurationParser.plugins_configurations
				.get("ICustomPublishingNeedsService"));
	}
}
