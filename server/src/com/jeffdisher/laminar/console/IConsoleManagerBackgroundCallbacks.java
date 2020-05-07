package com.jeffdisher.laminar.console;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;


/**
 * Callbacks sent by the console manager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IConsoleManagerBackgroundCallbacks {
	void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command);

	/**
	 * Called when a "stop" is issued on the console.
	 */
	void mainHandleStopCommand();
}
