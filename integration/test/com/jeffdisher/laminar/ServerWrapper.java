package com.jeffdisher.laminar;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * A wrapper over a Laminar server process for use in integration testing.
 * Note that this REQUIRES the "WRAPPER_SERVER_JAR" to be set in environment variables.
 * The variable points to the Laminar server jar which will be invoked directly to start the sub-process.
 * An optional "WRAPPER_VERBOSE" environment variable can be used to pass server output through (since it is normally
 * dropped).
 */
public class ServerWrapper {
	public static ServerWrapper startedServerWrapper(String serverName, int clusterPort, int clientPort, File storagePath) throws IOException {
		boolean verbose = (null != System.getenv("WRAPPER_VERBOSE"));
		PrintStream outTarget = verbose ? System.out : new PrintStream(new VoidOutputStream());
		PrintStream errTarget = verbose ? System.err : new PrintStream(new VoidOutputStream());
		String[] args = new String[]{"--cluster", Integer.toString(clusterPort)
				, "--client", Integer.toString(clientPort)
				, "--data", storagePath.getAbsolutePath()
		};
		return _startedServerWrapperWithStreams(serverName, args, outTarget, errTarget);
	}

	public static ServerWrapper startedServerWrapperRaw(String[] args, OutputStream errorStream) throws IOException {
		boolean verbose = (null != System.getenv("WRAPPER_VERBOSE"));
		PrintStream outTarget = verbose ? System.out : new PrintStream(new VoidOutputStream());
		PrintStream errTarget = new PrintStream(errorStream, true);
		return _startedServerWrapperWithStreams(null, args, outTarget, errTarget);
	}

	private static ServerWrapper _startedServerWrapperWithStreams(String serverName, String[] mainArgs, PrintStream outTarget, PrintStream errTarget) throws IOException {
		String javaLauncherPath = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
		String jarPath = System.getenv("WRAPPER_SERVER_JAR");
		if (null == jarPath) {
			throw new IllegalArgumentException("Missing WRAPPER_SERVER_JAR env var");
		}
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
		
		// Start the threads to drain the STDOUT and STDERR streams (we will just use the simple thread approach for this).
		DrainingThread stdout = new DrainingThread(serverName, outTarget, process.getInputStream());
		stdout.start();
		DrainingThread stderr = new DrainingThread(serverName, errTarget, process.getErrorStream());
		stderr.start();
		return new ServerWrapper(process, stdin, stdout, stderr);
	}


	private final Process _process;
	private final PrintStream _stdin;
	private final DrainingThread _stdout;
	private final DrainingThread _stderr;

	private ServerWrapper(Process process, PrintStream stdin, DrainingThread stdout, DrainingThread stderr) {
		_process = process;
		_stdin = stdin;
		_stdout = stdout;
		_stderr = stderr;
	}

	public int stop() throws InterruptedException {
		_stdin.println("stop");
		int exitStatus = _process.waitFor();
		_stdout.join();
		_stderr.join();
		_stdin.close();
		return exitStatus;
	}


	private static class DrainingThread extends Thread {
		private final String _linePrefix;
		private final PrintStream _sink;
		private final InputStream _source;
		
		public DrainingThread(String linePrefix, PrintStream sink, InputStream source) {
			_linePrefix = linePrefix;
			_sink = sink;
			_source = source;
		}
		
		@Override
		public void run() {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(_source));
				String line = reader.readLine();
				String prefix = (null != _linePrefix) ? (_linePrefix + ": ") : "";
				while (null != line) {
					_sink.println(prefix + line);
					line = reader.readLine();
				}
				_source.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(99);
			}
		}
	}


	private static class VoidOutputStream extends OutputStream {
		@Override
		public void write(int arg0) throws IOException {
			// Write nowhere.
		}
	}
}
