package com.jeffdisher.laminar;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.jeffdisher.laminar.utils.Assert;


/**
 * A wrapper over a Laminar server process for use in integration testing.
 * Note that this REQUIRES the "WRAPPER_SERVER_JAR" to be set in environment variables.
 * The variable points to the Laminar server jar which will be invoked directly to start the sub-process.
 * An optional "WRAPPER_VERBOSE" environment variable can be used to pass server output through (since it is normally
 * dropped).
 */
public class ServerWrapper {
	public static ServerWrapper startedServerWrapper(String serverName, int clusterPort, int clientPort, File storagePath) throws IOException {
		// We will use a default address, here.
		String localhost = InetAddress.getLocalHost().getHostAddress();
		return _startedServerWrapper(serverName, null, localhost, clusterPort, clientPort, storagePath);
	}

	public static ServerWrapper startedServerWrapperWithUuid(String serverName, UUID serverUuid, int clusterPort, int clientPort, File storagePath) throws IOException {
		// We will use a default address, here.
		String localhost = InetAddress.getLocalHost().getHostAddress();
		return _startedServerWrapper(serverName, serverUuid, localhost, clusterPort, clientPort, storagePath);
	}

	public static ServerWrapper startedServerWrapperWithUuidAndIp(String serverName, UUID serverUuid, String ipToBindBoth, int clusterPort, int clientPort, File storagePath) throws IOException {
		return _startedServerWrapper(serverName, serverUuid, ipToBindBoth, clusterPort, clientPort, storagePath);
	}

	public static ServerWrapper startedServerWrapperRaw(String[] args, OutputStream errorStream) throws IOException {
		ProcessWrapper wrapper = ProcessWrapper.startedJavaProcessWithRawErr(errorStream, _getJarPath(), args);
		wrapper.startFiltering();
		return new ServerWrapper(wrapper);
	}

	private static ServerWrapper _startedServerWrapper(String serverName, UUID serverUuid, String ipToBindBoth, int clusterPort, int clientPort, File storagePath) throws IOException {
		String[] args = (null == serverUuid)
				? new String[]{"--clusterIp", ipToBindBoth
						, "--clusterPort", Integer.toString(clusterPort)
						, "--clientIp", ipToBindBoth
						, "--clientPort", Integer.toString(clientPort)
						, "--data", storagePath.getAbsolutePath()
						}
				: new String[]{"--uuid", serverUuid.toString()
						, "--clusterIp", ipToBindBoth
						, "--clusterPort", Integer.toString(clusterPort)
						, "--clientIp", ipToBindBoth
						, "--clientPort", Integer.toString(clientPort)
						, "--data", storagePath.getAbsolutePath()
						};
		ProcessWrapper wrapper = ProcessWrapper.startedJavaProcess(serverName, _getJarPath(), args);
		CountDownLatch startLatch = wrapper.filterStdout("Laminar ready for leader connection or config upload...");
		wrapper.startFiltering();
		try {
			startLatch.await();
		} catch (InterruptedException e) {
			// We don't use interruption in our tests.
			throw Assert.unexpected(e);
		}
		return new ServerWrapper(wrapper);
	}

	private static String _getJarPath() {
		String jarPath = System.getenv("WRAPPER_SERVER_JAR");
		if (null == jarPath) {
			throw new IllegalArgumentException("Missing WRAPPER_SERVER_JAR env var");
		}
		if (!new File(jarPath).exists()) {
			throw new IllegalArgumentException("JAR \"" + jarPath + "\" doesn't exist");
		}
		return jarPath;
	}


	private final ProcessWrapper _process;

	private ServerWrapper(ProcessWrapper process) {
		_process = process;
	}

	public int stop() throws InterruptedException {
		_process.sendStdin("stop");
		return _process.waitForTermination();
	}
}
