package org.moquette.configurationmanager.plugins;

/**
 * To save the types of plugins. For instance, now there is a plugin to manage some required job
 * before publishing them message.
 * @author williamkinaan
 * 
 */
public enum PluginConfigurationTypes {
	ICustomPublishingNeedsService,
	ICustomAuthenticationService,
	ICustomAuthorizationService,
	IConDisService,
}
