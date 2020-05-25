package com.jeffdisher.laminar;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.jeffdisher.laminar.utils.Assert;


/**
 * A wrapper over an arbitrary process to allow high-level management of its STDIO and waiting for termination.
 * Of specific note is the ability to create Filter objects which will allow a test to wait on a certain string
 * appearing on STDOUT.
 * An optional "WRAPPER_VERBOSE" environment variable can be used to pass server output through (since it is normally
 * dropped).
 */
public class ProcessWrapper {
	/**
	 * Returns the started Java process but note that filtering is not yet enabled so startFiltering() must be called
	 * after any initial filtering setup in order for the process to run properly.
	 * 
	 * @param processName The name prefix which will be added to output when running in a verbose mode.
	 * @param jarPath The path of the JAR to execute.
	 * @param mainArgs The arguments to the main class of the JAR.
	 * @return The running process with filtering not yet enabled.
	 * @throws IOException Starting the process failed.
	 */
	public static ProcessWrapper startedJavaProcess(String processName, String jarPath, String... mainArgs) throws IOException {
		boolean verbose = (null != System.getenv("WRAPPER_VERBOSE"));
		PrintStream outTarget = verbose ? System.out : null;
		PrintStream errTarget = verbose ? System.err : null;
		return _startedJavaProcess(processName, outTarget, errTarget, jarPath, mainArgs);
	}

	/**
	 * Used in some special-cases to run tests such that the STDERR of the sub-process can be directly read by the test.
	 * 
	 * @param errorStream The STDERR stream to use for the sub-process.
	 * @param jarPath The path of the JAR to execute.
	 * @param mainArgs The arguments to the main class of the JAR.
	 * @return The running process with filtering not yet enabled.
	 * @throws IOException Starting the process failed.
	 */
	public static ProcessWrapper startedJavaProcessWithRawErr(OutputStream errorStream, String jarPath, String... mainArgs) throws IOException {
		boolean verbose = (null != System.getenv("WRAPPER_VERBOSE"));
		PrintStream outTarget = verbose ? System.out : null;
		// We always want to flush immediately to this error stream.
		PrintStream errTarget = new PrintStream(errorStream, true);
		return _startedJavaProcess(null, outTarget, errTarget, jarPath, mainArgs);
	}


	private static ProcessWrapper _startedJavaProcess(String linePrefix, PrintStream stdout, PrintStream stderr, String jarPath, String... mainArgs) throws IOException {
		String javaLauncherPath = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
		
		if (!new File(jarPath).exists()) {
			throw new IllegalArgumentException("JAR \"" + jarPath + "\" doesn't exist");
		}
		
		// Start the processes.
		List<String> args = new LinkedList<>();
		args.add(javaLauncherPath);
		args.add("-jar");
		args.add(jarPath);
		args.addAll(Arrays.asList(mainArgs));
		Process process = new ProcessBuilder(args).start();
		
		// Auto-flush stdin since it is used for commands.
		PrintStream stdin = new PrintStream(process.getOutputStream(), true);
		
		// Start the threads to filter the STDOUT and STDERR streams (we will just use the simple thread approach for this).
		FilteringThread filteredOut = new FilteringThread(process.getInputStream(), linePrefix, stdout);
		FilteringThread filteredErr = new FilteringThread(process.getErrorStream(), linePrefix, stderr);
		return new ProcessWrapper(process, stdin, filteredOut, filteredErr);
	}


	private final Process _process;
	private final PrintStream _stdin;
	private final FilteringThread _stdout;
	private final FilteringThread _stderr;

	private ProcessWrapper(Process process, PrintStream stdin, FilteringThread stdout, FilteringThread stderr) {
		_process = process;
		_stdin = stdin;
		_stdout = stdout;
		_stderr = stderr;
	}

	/**
	 * Adds a new filter to STDOUT so the caller can wait on when the string appears.
	 * 
	 * @param filter The string to watch for.
	 * @return A latch to use to wait on this appearing in STDOUT.
	 */
	public CountDownLatch filterStdout(String filter) {
		return _stdout.addFilter(filter);
	}

	/**
	 * Starts the underlying filter operation on the process's output streams.  Note that this MUST be called after any
	 * initial filters are setup or the process may stall when its output buffers fill.
	 */
	public void startFiltering() {
		_stdout.start();
		_stderr.start();
	}

	/**
	 * Sends the given string into the process's STDIN, on a new line.
	 * 
	 * @param string The string to send on its own line to the STDIN.
	 */
	public void sendStdin(String string) {
		_stdin.println(string);
	}

	/**
	 * Waits for the process to terminate and cleans up any internal resources.
	 * 
	 * @return The exit status of the sub-process.
	 * @throws InterruptedException If an interrupt was received while waiting.
	 */
	public int waitForTermination() throws InterruptedException {
		int exitStatus = _process.waitFor();
		_stdout.join();
		_stderr.join();
		_stdin.close();
		return exitStatus;
	}


	private static class FilteringThread extends Thread {
		private final InputStream _source;
		private final String _linePrefix;
		private final PrintStream _sink;
		private final Map<String, CountDownLatch> _filters;
		
		public FilteringThread(InputStream source, String linePrefix, PrintStream sink) {
			_source = source;
			_linePrefix = (null != linePrefix) ? (linePrefix + ": ") : "";
			_sink = sink;
			_filters = new HashMap<>();
		}
		
		public synchronized CountDownLatch addFilter(String filter) {
			CountDownLatch newLatch = new CountDownLatch(1);
			CountDownLatch previous = _filters.put(filter, newLatch);
			// We currently don't handle over-writing filters or tracking a list of them.
			Assert.assertTrue(null == previous);
			return newLatch;
		}
		
		@Override
		public void run() {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(_source));
				String line = reader.readLine();
				while (null != line) {
					// Check the filters.
					synchronized (this) {
						Set<String> matchedFilters = new HashSet<>();
						for (Map.Entry<String, CountDownLatch> filter : _filters.entrySet()) {
							String key = filter.getKey();
							if (line.contains(key)) {
								matchedFilters.add(key);
							}
						}
						for (String matched : matchedFilters) {
							CountDownLatch latch = _filters.remove(matched);
							latch.countDown();
						}
					}
					// If we have a sink, pass this line down to it.
					if (null != _sink) {
						_sink.println(_linePrefix + line);
					}
					line = reader.readLine();
				}
				_source.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(99);
			}
		}
	}
}
