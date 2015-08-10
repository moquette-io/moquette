/**
 * 
 */
package org.moquette.plugin.manager.plugins;

import java.util.Properties;
import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.io.BufferedReader;
import java.io.IOException;

import org.moquette.plugin.manager.exception.PluginNotCompatibleException;
import org.moquette.plugin.manager.exception.PluginNotFoundException;
import org.moquette.plugin.manager.exception.WrongPluginTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author williamkinaan This class is to parse the plugin file, which is
 *         plugins.conf
 */
public class PluginConfigurationParser {
	private static final Logger LOG = LoggerFactory
			.getLogger(PluginConfigurationParser.class);
	public static Properties plugins_configurations = new Properties();

	public PluginConfigurationParser() {

	}

	public void parse() {
		ClassLoader classLoader = new PluginConfigurationParser().getClass()
				.getClassLoader();

		File filePluginsConfiguration = new File(classLoader.getResource(
				"plugins.conf").getFile());
		if (!filePluginsConfiguration.exists()) {
			LOG.warn(String
					.format("parsing not existing file %s, so fallback on default plugin configurations!",
							filePluginsConfiguration.getAbsolutePath()));
			createDefaults();
			return;
		}
		FileReader reader;
		try {
			reader = new FileReader(filePluginsConfiguration);
			parse(reader);
		} catch (FileNotFoundException e) {
			LOG.warn(String
					.format("parsing not existing file %s, so fallback on default plugins configurations!",
							filePluginsConfiguration.getAbsolutePath()));
			LOG.warn(e.getMessage(), e);
			createDefaults();
		} catch (ParseException e) {
			LOG.error(String
					.format("parsing wrong formated file %s, so fallback on default plugins configurations!",
							filePluginsConfiguration.getAbsolutePath()));
			LOG.warn(e.getMessage(), e);
			createDefaults();
		}
	}

	/**
	 * 
	 * @param reader
	 * @throws ParseException
	 *             when the file is not well formatted
	 */
	void parse(Reader reader) throws ParseException {
		if (reader == null) {
			LOG.warn("parsing NULL reader, so fallback on default plugins configurations!");
			createDefaults();
			return;
		}
		BufferedReader br = new BufferedReader(reader);
		String line;
		try {
			while ((line = br.readLine()) != null) {
				int commentMarker = line.indexOf('#');
				if (commentMarker != -1) {
					if (commentMarker == 0) {
						// skip its a comment
						continue;
					} else {
						// it's a malformed comment
						throw new ParseException(line, commentMarker);
					}
				} else {
					if (line.isEmpty() || line.matches("^\\s*$")) {
						// skip it's a black line
						continue;
					}

					// split till the first space
					int deilimiterIdx = line.indexOf(' ');
					String key = line.substring(0, deilimiterIdx).trim();
					String value = line.substring(deilimiterIdx).trim();

					try {
						PluginTypes type = PluginTypes.getTypeFromString(key);
						validatePlugin(type, value);
						LOG.info(String.format(
								"Found a plugin, key = %s, value = %s", key,
								value));
						plugins_configurations.put(key, value);
					} catch (WrongPluginTypeException e) {
						LOG.warn(String
								.format("Iqnoring an entry %s  because it is not a plugin type. To see the avaiable plugin types, check PluginTypes enum",
										key));
						LOG.warn(e.getMessage(), e);
					} catch (PluginNotFoundException e) {
						LOG.error(e.getMessage(), e);
					} catch (PluginNotCompatibleException e) {
						LOG.error(e.getMessage(), e);
					}
				}
			}
		} catch (IOException ex) {
			throw new ParseException("Failed to read", 1);
		}

	}

	private boolean validatePlugin(PluginTypes type, String value)
			throws PluginNotFoundException, PluginNotCompatibleException {
		Class myClass;
		try {
			myClass = Class.forName(value);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			throw new PluginNotFoundException(String.format(
					"Couldn't find class %s", value));
		}

		if (type.getPluginInterfaceForThisPluginType()
				.isAssignableFrom(myClass)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String
						.format("found class %s for pluging type %s and validated correctly",
								value, type.toString()));
			}
			return true;
		} else {
			throw new PluginNotCompatibleException(String.format(
					"Pluging provided %s is not compatible with type %s",
					value, type.toString()));
		}

	}

	private void createDefaults() {
		plugins_configurations
				.put(PluginTypes.ICustomPublishingNeedsService,
						"org.moquette.configurationmanager.examples.DefaulPublishingNeesd");
	}
}
