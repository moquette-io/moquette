package org.moquette.plugin.manager.exception;

public class MoquettePluginManagerException extends Exception {

	private static final long serialVersionUID = 1L;

	public MoquettePluginManagerException(String message) {
		super(message);
	}

	public MoquettePluginManagerException(String message, Throwable throwable) {
		super(message, throwable);
	}

}
