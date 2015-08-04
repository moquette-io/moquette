package org.moquette.configurationmanager.plugins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author williamkinaan This class is to parse the plugin configuration file
 *         (plugins.conf)
 */

public class PluginConfigurationParser {
	private static final Logger LOG = LoggerFactory
			.getLogger(PluginConfigurationParser.class);

	public Properties plugins_configurations = new Properties();

	public PluginConfigurationParser() {

	}

	public void parse() {
		String configPath = System.getProperty("moquette.path", null);
		File filePluginsConfiguration = new File(configPath,
				"config/plugins.conf");
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
			createDefaults();
		} catch (ParseException e) {
			LOG.error(String
					.format("parsing wrong formated file %s, so fallback on default plugins configurations!",
							filePluginsConfiguration.getAbsolutePath()));
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

					plugins_configurations.put(key, value);
				}
			}
		} catch (IOException ex) {
			throw new ParseException("Failed to read", 1);
		}

	}

	private void createDefaults() {
		plugins_configurations.put(
				PluginConfigurationTypes.ICustomPublishingNeedsService,
				"org.moquette.configurationmanager.examples.DefaulPublishingNeesd");
	}
}
