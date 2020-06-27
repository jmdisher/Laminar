package com.jeffdisher.laminar.performance;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.contracts.DoNothing;
import com.jeffdisher.laminar.types.CommitInfo;
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
		// We just want the one topic.
		TopicName topic = TopicName.fromString("test");
		byte[] code = TEST_PROGRAMMABLE
				? ContractPackager.createJarForClass(DoNothing.class)
				: new byte[0];
		byte[] arguments = new byte[0];
		
		// Create the components for the test run.
		Engine.TopicConfiguration topicToCreate = new Engine.TopicConfiguration(topic, code, arguments);
		Runner[] clients = new Runner[clientCount];
		for (int i = 0; i < clientCount; ++i) {
			clients[i] = new Runner(topic, messagesPerClient);
		}
		
		// Execute performance run.
		long[] nanoTimes = Engine.runWithUnitsOnThreads(_folder.newFolder(), "Write", nodeCount, messagesPerClient, new Engine.TopicConfiguration[] {topicToCreate}, clients);
		
		// Collect the maximum value, since we want to treat the block of clients as 1 data point.
		// If any of these were -1L, there was an error.
		long totalTimeNanos = 0L;
		for (long time : nanoTimes) {
			Assert.assertNotEquals(-1L, time);
			totalTimeNanos = Long.max(totalTimeNanos, time);
		}
		long perMessageTimeMicros = (totalTimeNanos / (clientCount * messagesPerClient)) / 1_000;
		
		System.out.println("PERF:");
		System.out.println("\tNode count: " + nodeCount);
		System.out.println("\tClient count: " + clientCount);
		System.out.println("\tMessages per client: " + messagesPerClient);
		System.out.println("\tTotal time: " + (totalTimeNanos / 1_000_000) + " ms");
		System.out.println("\tTime per message: " + perMessageTimeMicros + " us");
		return perMessageTimeMicros;
	}


	private static class Runner implements IPerformanceUnit {
		private final TopicName _topic;
		private final ClientResult[] _results;
		public Runner(TopicName topic, int iterationsToCapture) {
			_topic = topic;
			_results = new ClientResult[iterationsToCapture];
		}
		@Override
		public void startWithClient(ClientConnection client) throws Throwable {
			// Do nothing.
		}
		@Override
		public void runIteration(ClientConnection client, int iteration) throws Throwable {
			// Send the message and store result for later.
			byte[] key = new byte[] {1,2,3,4};
			byte[] value = new byte[] {(byte)iteration};
			_results[iteration] = client.sendPut(_topic, key, value);
		}
		@Override
		public void finishTestUnderTime(ClientConnection client) throws Throwable {
			// Wait on all results.
			for (ClientResult result : _results) {
				CommitInfo info = result.waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			}
		}
		@Override
		public void exceptionInRun(Throwable t) {
			t.printStackTrace();
		}
	}
}
