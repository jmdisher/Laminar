package com.jeffdisher.laminar;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.state.NodeState;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;


/**
 * The main class of a Laminar node.
 * Required command-line arguments:
 * -"--clientIp" &lt;ip&gt; - the IP address which will be bound for listening to client connections
 * -"--clientPort" &lt;port&gt; - the port which will be bound for listening to client connections
 * -"--clusterIp" &lt;port&gt; - the IP address which will be bound for listening to connections from other cluster members
 * -"--clusterPort" &lt;port&gt; - the port which will be bound for listening to connections from other cluster members
 * -"--data" - the directory which will be used for storing incoming and committed stream files
 * Optional arguments:
 * -"--uuid" &lt;UUID&gt; - forces the UUID of the server to be this instead of randomly generated on start-up
 * NOTE:  Port settings will be made optional in the future (mostly just for testing multiple nodes on one machine).
 */
public class Laminar {
	public static void main(String[] args) {
		// The way this starts up is to parse command-line options, bind required ports, output details of the start-up,
		// ensure the writability of the data directory, start all background components, print that start-up has
		// completed, and then the main thread transitions into its state management mode where it runs until shutdown.
		
		// Parse command-line options.
		String clientIpString = parseOption(args, "--clientIp");
		String clientPortString = parseOption(args, "--clientPort");
		String clusterIpString = parseOption(args, "--clusterIp");
		String clusterPortString = parseOption(args, "--clusterPort");
		String dataDirectoryName = parseOption(args, "--data");
		String uuidString = parseOption(args, "--uuid");
		
		// Create the UUID this node will use (in config, etc).
		UUID serverUuid = (null == uuidString)
				? UUID.randomUUID()
				: UUID.fromString(uuidString);
		System.out.println("Laminar server starting up:  " + serverUuid);
		
		// Make sure we were given our required options.
		if ((null == clientIpString) || (null == clientPortString) || (null == clusterIpString) || (null == clusterPortString) || (null == dataDirectoryName)) {
			failStart("Missing options!  Usage:  Laminar --clientIp <client_ip> --clientPort <client_port> --clusterIp <cluster_ip> --clusterPort <cluster_port> --data <data_directory_path>");
		}
		
		// Parse IPs and ports.
		InetSocketAddress clientSocketAddress = _parseIpAndPort(clientIpString, clientPortString);
		InetSocketAddress clusterSocketAddress = _parseIpAndPort(clusterIpString, clusterPortString);
		
		// Bind ports.
		ServerSocketChannel clientSocket = null;
		ServerSocketChannel clusterSocket = null;
		try {
			clientSocket = bindLocalPort(clientSocketAddress);
			clusterSocket = bindLocalPort(clusterSocketAddress);
		} catch (IOException e) {
			failStart("Failure binding required port: " + e.getLocalizedMessage());
		}
		
		// Check data directory.
		File dataDirectory = new File(dataDirectoryName);
		if (!dataDirectory.exists()) {
			boolean didCreate = dataDirectory.mkdirs();
			if (!didCreate) {
				failStart("Could not create data directory (or parents): \"" + dataDirectoryName +"\"");
			}
		}
		if (!dataDirectory.canWrite()) {
			failStart("Data directory not writable: \"" + dataDirectoryName +"\"");
		}
		
		// Log the successful start-up.
		System.out.println("Client-facing socket bound: " + clientSocketAddress);
		System.out.println("Cluster-facing socket bound: " + clusterSocketAddress);
		System.out.println("Data directory configured: " + dataDirectoryName);
		
		// By this point, all requirements of the system should be satisfied so create the subsystems.
		// First, the core NodeState and the background thread callback handlers for the managers.
		// Note that we need to create an "initial config" which we will use until we get a cluster update from a client or another node starts sending updates.
		ConfigEntry self = new ConfigEntry(serverUuid, clusterSocketAddress, clientSocketAddress);
		ClusterConfig initialConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {self});
		NodeState thisNodeState = new NodeState(initialConfig);
		// We also want to install an uncaught exception handler to make sure background thread failures are fatal.
		Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				System.err.println("FATAL UNCAUGHT ERROR ON THREAD: " + t);
				e.printStackTrace(System.err);
				System.exit(99);
			}
		});
		
		// Now, create the managers.
		ClientManager clientManager = null;
		try {
			clientManager = new ClientManager(self, clientSocket, thisNodeState);
		} catch (IOException e1) {
			// Not sure how creating the Selector would fail but we can handle it since we haven't started, yet.
			failStart("Failure creating ClientManager: " + e1.getLocalizedMessage());
		}
		ClusterManager clulsterManager = null;
		try {
			clulsterManager = new ClusterManager(self, clusterSocket, thisNodeState);
		} catch (IOException e1) {
			// Not sure how creating the Selector would fail but we can handle it since we haven't started, yet.
			failStart("Failure creating ClusterManager: " + e1.getLocalizedMessage());
		}
		DiskManager diskManager = new DiskManager(dataDirectory, thisNodeState);
		ConsoleManager consoleManager = new ConsoleManager(System.out, System.in, thisNodeState);
		
		// All the components are ready so we can now register the managers with it.
		thisNodeState.registerClientManager(clientManager);
		thisNodeState.registerClusterManager(clulsterManager);
		thisNodeState.registerDiskManager(diskManager);
		thisNodeState.registerConsoleManager(consoleManager);
		
		// Start all background threads and other manager processes.
		clientManager.startAndWaitForReady();
		clulsterManager.startAndWaitForReady();
		diskManager.startAndWaitForReady();
		consoleManager.startAndWaitForReady();
		
		// We are now ready so enter the initial state.
		System.out.println("Laminar ready for leader connection or config upload...");
		thisNodeState.runUntilShutdown();
		
		// The node state has entered a shutdown state so notify the user and close everything.
		System.out.println("Laminar shutting down...");
		clientManager.stopAndWaitForTermination();
		clulsterManager.stopAndWaitForTermination();
		diskManager.stopAndWaitForTermination();
		consoleManager.stopAndWaitForTermination();
		
		// Close the resources we created - we just log if there are issues, and proceed.
		System.out.println("Laminar threads shutdown.  Closing sockets and terminating...");
		try {
			clientSocket.close();
		} catch (IOException e) {
			System.out.println("Client socket close exception: " + e.getLocalizedMessage());
		}
		try {
			clusterSocket.close();
		} catch (IOException e) {
			System.out.println("Cluster socket close exception: " + e.getLocalizedMessage());
		}
	}

	private static ServerSocketChannel bindLocalPort(InetSocketAddress clientAddress) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		socket.bind(clientAddress);
		return socket;
	}

	private static void failStart(String message) {
		System.err.println("Fatal start-up error: " + message);
		System.exit(1);
	}

	private static String parseOption(String[] args, String option) {
		String result = null;
		for (int index = 0; (null == result) && (index < (args.length - 1)); ++index) {
			if (option.equals(args[index])) {
				result = args[index+1];
			}
		}
		return result;
	}

	private static InetSocketAddress _parseIpAndPort(String ipString, String portString) {
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(ipString);
		} catch (UnknownHostException e) {
			failStart("Invalid hostname or IP provided: \"" + ipString + "\"");
		}
		int port = Integer.parseInt(portString);
		return ClusterConfig.cleanSocketAddress(new InetSocketAddress(ip, port));
	}
}
