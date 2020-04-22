package com.jeffdisher.laminar.console;


/**
 * Callbacks sent by the console manager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IConsoleManagerBackgroundCallbacks {
	/**
	 * Called when a "stop" is issued on the console.
	 */
	void handleStopCommand();
}
