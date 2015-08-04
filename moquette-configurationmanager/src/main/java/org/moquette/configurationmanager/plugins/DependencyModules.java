package org.moquette.configurationmanager.plugins;

import java.util.Properties;


import com.google.inject.AbstractModule;


/**
 * Google Guice module to handle the dependence injection
 * 
 * @author williamkinaan
 *
 */
public class DependencyModules extends AbstractModule {
	Properties properties;

	public DependencyModules(Properties properties) {
		this.properties = properties;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		try {
			bind(ICustomPublishingNeeds.class)
					.to((Class<? extends ICustomPublishingNeeds>) Class.forName(this.properties
							.getProperty(PluginConfigurationTypes.ICustomPublishingNeedsService
									.toString())));
			if (properties
					.containsKey(PluginConfigurationTypes.ICustomAuthorizationService
							.toString())) {
			//	bind(IAuthorizator.class)
				//		.to((Class<? extends IAuthorizator>) Class.forName(this.properties
					//			.getProperty(PluginConfigurationTypes.ICustomAuthorizationService
						//				.toString())));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
