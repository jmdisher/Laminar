package com.jeffdisher.laminar.performance;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.ServerWrapper;
import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.contracts.DoNothing;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Does a performance run just using JUnit as an entry-point.  This name starts with "Perf" instead of "Test" so that it
 * can be run in a different Ant target.  This is because this should be allowed to take a long time to run.
 * This performance test is specifically testing write throughput across clusters of various sizes with different
 * numbers of concurrent clients.
 * The servers are started as external processes but all the clients are run in a single thread, inside the test.  We
 * send all the messages before waiting for any of them to commit in order to maximize possible server-side concurrency
 * and reduce any single-thread lock-step across a single thread in this test.
 * The key result being measured is the average number of microseconds for a single message to commit (which is lower
 * the more messages are sent).
 * This requires env var to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 */
public class PerfWritePerformance {
	/**
	 * Node counts to use in a test run.
	 */
	private static final int[] NODES = new int[] { 1, 3, 5, 7 };
	/**
	 * Concurrent client counts to use in a test run.
	 */
	private static final int[] CLIENTS = new int[] { 1, 3, 5 };
	/**
	 * Number of messages each client in a test run should send.
	 */
	private static final int[] MESSAGES_PER_CLIENT = new int[] { 100, 1000, 10000 };
	/**
	 * Set to true to test a programmable topic, false to test a vanilla one.
	 */
	private static boolean TEST_PROGRAMMABLE = true;

	@ClassRule
	public static TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void perfRun() throws Throwable {
		// Collect all data.
		long perMessageTimeMicros[][][] = new long[NODES.length][CLIENTS.length][MESSAGES_PER_CLIENT.length];
		for (int i = 0; i < NODES.length; ++i) {
			int nodeCount = NODES[i];
			for (int j = 0; j < CLIENTS.length; ++j) {
				int clientCount = CLIENTS[j];
				for (int k = 0; k < MESSAGES_PER_CLIENT.length; ++k) {
					int messagesPerClient = MESSAGES_PER_CLIENT[k];
					perMessageTimeMicros[i][j][k] = _runTestLoad(nodeCount, clientCount, messagesPerClient);
				}
			}
		}
		
		// Print CSV segments - the outer-most batch is the messages per client since that is mostly about how washed-out constant costs are.
		System.out.println("CSV OUTPUT");
		for (int k = 0; k < MESSAGES_PER_CLIENT.length; ++k) {
			int messagesPerClient = MESSAGES_PER_CLIENT[k];
			System.out.println("Test run with " + messagesPerClient + " messages per client");
			for (int clientCount : CLIENTS) {
				System.out.print("," + clientCount + " clients");
			}
			System.out.println();
			for (int i = 0; i < NODES.length; ++i) {
				int nodeCount = NODES[i];
				System.out.print(nodeCount + " nodes");
				for (int j = 0; j < CLIENTS.length; ++j) {
					System.out.print("," + perMessageTimeMicros[i][j][k]);
				}
				System.out.println();
			}
			System.out.println();
		}
	}


	private static long _runTestLoad(int nodeCount, int clientCount, int messagesPerClient) throws Throwable {
		// Create the servers and find their initial configs.
		ServerWrapper[] servers = new ServerWrapper[nodeCount];
		ConfigEntry[] configs = new ConfigEntry[nodeCount];
		for (int i = 0; i < servers.length; ++i) {
			int clusterPort = 2000 + i;
			int clientPort = 3000 + i;
			servers[i] = ServerWrapper.startedServerWrapper("perf_nodes" + nodeCount + "_clients" + clientCount +"_load" + messagesPerClient + "-" + i, clusterPort, clientPort, _folder.newFolder());
			InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), clientPort);
			try (ClientConnection configClient = ClientConnection.open(address)) {
				configClient.waitForConnectionOrFailure();
				configs[i] = configClient.getCurrentConfig().entries[0];
			}
		}
		
		// Send the new config, wait for the cluster to form, and create the topic for the test.
		TopicName topic = TopicName.fromString("test");
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 3000);
		try (ClientConnection configClient = ClientConnection.open(address)) {
			configClient.waitForConnectionOrFailure();
			Assert.assertEquals(CommitInfo.Effect.VALID, configClient.sendUpdateConfig(ClusterConfig.configFromEntries(configs)).waitForCommitted().effect);
			
			if (TEST_PROGRAMMABLE) {
				byte[] code = ContractPackager.createJarForClass(DoNothing.class);
				byte[] arguments = new byte[0];
				Assert.assertEquals(CommitInfo.Effect.VALID, configClient.sendCreateProgrammableTopic(topic, code, arguments).waitForCommitted().effect);
			} else {
				Assert.assertEquals(CommitInfo.Effect.VALID, configClient.sendCreateTopic(topic).waitForCommitted().effect);
			}
		}
		
		// Open all real client connections.
		ClientConnection[] clients = new ClientConnection[clientCount];
		for (int i = 0; i < clients.length; ++i) {
			clients[i] = ClientConnection.open(address);
			clients[i].waitForConnectionOrFailure();
		}
		
		// Run the core of the test.
		long start = System.currentTimeMillis();
		_sendPut(topic, clients, messagesPerClient);
		long end = System.currentTimeMillis();
		
		// Shut down.
		for (int i = 0; i < clients.length; ++i) {
			clients[i].close();
		}
		for (int i = 0; i < servers.length; ++i) {
			Assert.assertEquals(0, servers[i].stop());
		}
		
		// Print report.
		long totalTimeMillis = (end - start);
		long perMessageTimeMicros = (1000 * totalTimeMillis / (clientCount * messagesPerClient));
		
		System.out.println("PERF:");
		System.out.println("\tNode count: " + nodeCount);
		System.out.println("\tClient count: " + clientCount);
		System.out.println("\tMessages per client: " + messagesPerClient);
		System.out.println("\tTotal time: " + totalTimeMillis + " ms");
		System.out.println("\tTime per message: " + perMessageTimeMicros + " us");
		return perMessageTimeMicros;
	}

	private static void _sendPut(TopicName topic, ClientConnection[] clients, int count) throws InterruptedException {
		byte[] key = new byte[] {1,2,3,4};
		ClientResult[][] results = new ClientResult[count][clients.length];
		for (int i = 0; i < count; ++i) {
			for (int j = 0; j < clients.length; ++j) {
				results[i][j] = clients[j].sendPut(topic, key, new byte[] {(byte)i});
			}
		}
		for (int i = 0; i < count; ++i) {
			for (int j = 0; j < clients.length; ++j) {
				CommitInfo info = results[i][j].waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			}
		}
	}
}
