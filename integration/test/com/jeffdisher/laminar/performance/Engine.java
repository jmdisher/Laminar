package com.jeffdisher.laminar.performance;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;

import com.jeffdisher.laminar.ServerWrapper;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Common support for performance tests.
 * 
 * This requires env var to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 */
public class Engine {
	public static long[] runWithUnitsOnThreads(File topLevelDirectory, String name, int nodeCount, int messagesPerClient, TopicConfiguration[] topicsToCreate, IPerformanceUnit[] units) throws IOException, InterruptedException {
		// Create the servers and find their initial configs.
		ServerWrapper[] servers = new ServerWrapper[nodeCount];
		ConfigEntry[] configs = new ConfigEntry[nodeCount];
		for (int i = 0; i < servers.length; ++i) {
			int clusterPort = 2000 + i;
			int clientPort = 3000 + i;
			servers[i] = ServerWrapper.startedServerWrapper(name + "_perf_nodes" + nodeCount + "_clients" + units.length +"_load" + messagesPerClient + "-" + i, clusterPort, clientPort, new File(topLevelDirectory, name + "_" + i));
			InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), clientPort);
			try (ClientConnection configClient = ClientConnection.open(address)) {
				configClient.waitForConnectionOrFailure();
				configs[i] = configClient.getCurrentConfig().entries[0];
			}
		}
		
		// Upload config and create topics.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 3000);
		try (ClientConnection configClient = ClientConnection.open(address)) {
			configClient.waitForConnectionOrFailure();
			Assert.assertEquals(CommitInfo.Effect.VALID, configClient.sendUpdateConfig(ClusterConfig.configFromEntries(configs)).waitForCommitted().effect);
			for (TopicConfiguration topicConfig : topicsToCreate) {
				Assert.assertEquals(CommitInfo.Effect.VALID, configClient.sendCreateProgrammableTopic(topicConfig.name, topicConfig.code, topicConfig.arguments).waitForCommitted().effect);
			}
		}
		
		// Start the client threads and announce to the units that they should begin.
		CountDownLatch startLatch = new CountDownLatch(units.length);
		long[] nanoTimings = new long[units.length];
		Thread[] unitThreads = new Thread[units.length];
		for (int i = 0; i < unitThreads.length; ++i) {
			final int index = i;
			IPerformanceUnit unit = units[i];
			unitThreads[i] = new Thread(() -> {
				try {
					try (ClientConnection client = ClientConnection.open(address)) {
						client.waitForConnectionOrFailure();
						unit.startWithClient(client);
						
						// Wait for synchronization.
						startLatch.countDown();
						
						// Do the test run.
						long startNanos = System.nanoTime();
						for (int j = 0; j < messagesPerClient; ++j) {
							unit.runIteration(client, j);
						}
						unit.finishTestUnderTime(client);
						long endNanos = System.nanoTime();
						nanoTimings[index] = endNanos - startNanos;
					}
				} catch (Throwable t) {
					unit.exceptionInRun(t);
					nanoTimings[index] = -1L;
				}
			}, name + "_unit" + i);
			unitThreads[i].start();
		}
		
		// Wait for completion.
		for (Thread unit : unitThreads) {
			unit.join();
		}
		
		// Stop servers.
		for (ServerWrapper wrapper : servers) {
			Assert.assertEquals(0, wrapper.stop());
		}
		
		// Complete - return output.
		return nanoTimings;
	}

	public static class TopicConfiguration {
		public final TopicName name;
		public final byte[] code;
		public final byte[] arguments;
		
		public TopicConfiguration(TopicName name, byte[] code, byte[] arguments) {
			this.name = name;
			this.code = code;
			this.arguments = arguments;
		}
	}
}
