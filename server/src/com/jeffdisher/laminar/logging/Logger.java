package com.jeffdisher.laminar.logging;

import java.io.PrintStream;


/**
 * To keep this simple, we just use a basic logger which is either verbose or silent.
 * In the silent mode, only calls which come in as "important" are written.
 */
public class Logger {
	private final PrintStream _output;
	private final boolean _verbose;

	public Logger(PrintStream output, boolean verbose) {
		_output = output;
		_verbose = verbose;
	}

	public void info(String message) {
		if (_verbose) {
			_output.println(message);
		}
	}

	public void important(String message) {
		_output.println(message);
	}
}
