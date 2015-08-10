/**
 * 
 */
package org.moquette.plugin.manager.plugins;

import org.moquette.plugin.manager.exception.WrongPluginTypeException;
import org.moquette.plugin.manager.plugins.custompublishingneeds.ICustomPublishingNeeds;

/**
 * @author williamkinaan
 * 
 */
public enum PluginTypes {

	ICustomPublishingNeedsService(ICustomPublishingNeeds.class);

	private Class<? extends IPlugin> iPlugin;

	private PluginTypes(Class<? extends IPlugin> iPlugin) {
		this.iPlugin = iPlugin;
	}

	public Class<? extends IPlugin> getPluginInterfaceForThisPluginType() {
		return this.iPlugin;
	}

	/**
	 * 
	 * @param value
	 *            is the string value that we want to know if the enum contains
	 *            a type with the same string's value.
	 * @return true if the parameter's string value is equal to one of the
	 *         already existed plugin types. false otherwise.
	 */
	public static boolean contains(String value) {
		for (PluginTypes c : PluginTypes.values()) {
			if (c.toString().equals(value))
				return true;
		}
		return false;
	}

	/**
	 * To get the enum value for a string input
	 * 
	 * @param value
	 *            : the string that we want to get a plugin type from
	 * @return the actual plugin type
	 * @throws WrongPluginTypeException
	 *             when the input string doesn't match any of the defined plugin
	 *             types.
	 */
	public static PluginTypes getTypeFromString(String value)
			throws WrongPluginTypeException {
		try {
			return PluginTypes.valueOf(value);
		} catch (IllegalArgumentException e) {
			throw new WrongPluginTypeException(
					String.format(
							"Exception couldn't find a plugin type for %s , please to see the available types, check PluginTypes enum",
							value));
		}
	}

}
