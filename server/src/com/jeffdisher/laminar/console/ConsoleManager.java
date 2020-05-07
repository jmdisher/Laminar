package com.jeffdisher.laminar.console;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Scanner;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Asynchronous abstraction over the STDIO of the starting console.
 * This allows interaction with and monitoring of the node.
 * 
 * Unfortunately, Java NIO doesn't seem to expose a way to manage the STDIO via a Selector (probably because this
 * paradigm isn't portable across all operating systems).  So, this will use a bit of a cheat:
 * -STDIN will be read in a background thread by polling for available bytes, periodically (no timeout or interruption
 *  seems to work, so polling is our only option)
 * -STDOUT will be directly written by the calling thread
 * -we will stop the background thread by setting a flag to stop polling
 */
public class ConsoleManager implements IConsoleManager {
	private static final long POLL_DELAY_MILLIS = 100L;

	private final PrintStream _out;
	private final InputStream _in;
	private final IConsoleManagerBackgroundCallbacks _callbacks;

	private boolean _keepRunning;
	private Thread _background;

	public ConsoleManager(PrintStream out, InputStream in, IConsoleManagerBackgroundCallbacks callbacks) {
		_out = out;
		_in = in;
		_callbacks = callbacks;
	}

	public void startAndWaitForReady() {
		_keepRunning = true;
		_background = new Thread() {
			@Override
			public void run() {
				try {
					_backgroundThreadMain();
				} catch (IOException e) {
					// We can't find an IOException on STDIN.
					throw Assert.unexpected(e);
				}
			}
		};
		_background.setName("Laminar console");
		_background.start();
	}

	public void stopAndWaitForTermination() {
		synchronized (this) {
			// Shut off the flag.
			_keepRunning = false;
			// Notify the thread, in case it is waiting.
			this.notifyAll();
		}
		// Wait for the thread to terminate.
		try {
			_background.join();
		} catch (InterruptedException e) {
			// We don't use interruption.
			Assert.unexpected(e);
		}
	}


	private void _backgroundThreadMain() throws IOException {
		// We use a super-simple dialect so we just keep a 1 KiB buffer waiting for a newline.
		byte[] buffer = new byte[1024];
		int index = 0;
		while (_shouldRunAfterWait()) {
			while (_in.available() > 0) {
				// There is something.
				if (index == buffer.length) {
					// Overflow, so print a warning and reset the buffer.
					_out.println("Error - line too long");
					Arrays.fill(buffer, (byte)0x0);
					index = 0;
				}
				byte value = (byte)_in.read();
				buffer[index] = value;
				index += 1;
				// If the string now ends in a line separator, parse it.
				String soFar = new String(buffer, 0, index, StandardCharsets.UTF_8);
				if (soFar.endsWith(System.lineSeparator())) {
					_parseLineAndSendCommand(soFar);
					Arrays.fill(buffer, (byte)0x0);
					index = 0;
				}
			}
		}
		// We fall out and return when we were told to stop running.
	}

	private void _parseLineAndSendCommand(String line) {
		try (Scanner scanner = new Scanner(line)) {
			if (scanner.hasNext()) {
				String next = scanner.next();
				// We only understand "stop".
				if (next.equalsIgnoreCase("stop")) {
					_callbacks.ioEnqueueConsoleCommandForMainThread((snapshot) -> _callbacks.mainHandleStopCommand());
				} else {
					_out.println("Command \"" + next + "\" not understood");
				}
			}
		}
	}

	private synchronized boolean _shouldRunAfterWait() {
		// We don't worry about spurious wake-up since we will just check the buffer and wait again.
		// (this is just for throttling, not for logic).
		if (_keepRunning) {
			try {
				this.wait(POLL_DELAY_MILLIS);
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
		return _keepRunning;
	}
}
