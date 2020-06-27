package com.jeffdisher.laminar.performance;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.contracts.EmulateNormal;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Does a performance run just using JUnit as an entry-point.  This name starts with "Perf" instead of "Test" so that it
 * can be run in a different Ant target.  This is because this should be allowed to take a long time to run.
 * This performance test is specifically testing write throughput across clusters of various sizes, different numbers of
 * concurrent clients, and programmable topics.
 * The key result being measured is the average number of microseconds for a single message to commit (which is lower
 * the more messages are sent).
 * 
 * This requires env var to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 * 
 * Set VERBOSE to output per-run stats.
 */
public class PerfWriteEmulated {
	/**
	 * Node counts to use in a test run.
	 */
	private static final int[] NODES = new int[] { 1, 3, 5 };
	/**
	 * Concurrent client counts to use in a test run.
	 */
	private static final int[] CLIENTS = new int[] { 1, 3, 5 };
	/**
	 * Number of messages each client in a test run should send.
	 */
	private static final int MESSAGES_PER_CLIENT = 10_000;

	@ClassRule
	public static TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void perfRun() throws Throwable {
		// Collect all data.
		long perMessageTimeMicros[][] = new long[NODES.length][CLIENTS.length];
		for (int i = 0; i < NODES.length; ++i) {
			int nodeCount = NODES[i];
			for (int j = 0; j < CLIENTS.length; ++j) {
				int clientCount = CLIENTS[j];
				perMessageTimeMicros[i][j] = _runTestLoad(nodeCount, clientCount, MESSAGES_PER_CLIENT);
			}
		}
		
		// Print CSV segments - the outer-most batch is the messages per client since that is mostly about how washed-out constant costs are.
		System.out.println("CSV OUTPUT");
		System.out.println("Test run with " + MESSAGES_PER_CLIENT + " messages per client");
		for (int clientCount : CLIENTS) {
			System.out.print("," + clientCount + " clients");
		}
		System.out.println();
		for (int i = 0; i < NODES.length; ++i) {
			int nodeCount = NODES[i];
			System.out.print(nodeCount + " nodes");
			for (int j = 0; j < CLIENTS.length; ++j) {
				System.out.print("," + perMessageTimeMicros[i][j]);
			}
			System.out.println();
		}
	}


	private static long _runTestLoad(int nodeCount, int clientCount, int messagesPerClient) throws Throwable {
		// We just want the one topic.
		TopicName topic = TopicName.fromString("test");
		byte[] code = ContractPackager.createJarForClass(EmulateNormal.class);
		byte[] arguments = new byte[0];
		
		// Create the components for the test run.
		Engine.TopicConfiguration topicToCreate = new Engine.TopicConfiguration(topic, code, arguments);
		Runner[] clients = new Runner[clientCount];
		for (int i = 0; i < clientCount; ++i) {
			clients[i] = new Runner(topic, messagesPerClient);
		}
		
		// Execute performance run.
		long[] nanoTimes = Engine.runWithUnitsOnThreads(_folder.newFolder(), "WriteProgrammable", nodeCount, messagesPerClient, new Engine.TopicConfiguration[] {topicToCreate}, clients);
		
		// Collect the maximum value, since we want to treat the block of clients as 1 data point.
		// If any of these were -1L, there was an error.
		long totalTimeNanos = 0L;
		for (long time : nanoTimes) {
			Assert.assertNotEquals(-1L, time);
			totalTimeNanos = Long.max(totalTimeNanos, time);
		}
		long perMessageTimeMicros = (totalTimeNanos / (clientCount * messagesPerClient)) / 1_000;
		
		if (null != System.getenv("VERBOSE")) {
			System.out.println("PERF (WriteProgrammable):");
			System.out.println("\tNode count: " + nodeCount);
			System.out.println("\tClient count: " + clientCount);
			System.out.println("\tMessages per client: " + messagesPerClient);
			System.out.println("\tTotal time: " + (totalTimeNanos / 1_000_000) + " ms");
			System.out.println("\tTime per message: " + perMessageTimeMicros + " us");
		}
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
			// NOTE:  Keys MUST be 32 bytes due to programmable topic requirements (check is in the AVM core).
			byte[] key = new byte[32];
			key[10] = 1;
			key[11] = 2;
			key[12] = 3;
			key[13] = 4;
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
