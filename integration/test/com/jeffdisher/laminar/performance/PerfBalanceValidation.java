package com.jeffdisher.laminar.performance;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.contracts.AccountBalanceValidation;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Does a performance run just using JUnit as an entry-point.  This name starts with "Perf" instead of "Test" so that it
 * can be run in a different Ant target.  This is because this should be allowed to take a long time to run.
 * This performance test is specifically testing write throughput across clusters of various sizes, and different
 * numbers of concurrent clients against a topic using the AccountBalanceValidation program.
 * Note that the program will be deployed with a large enough cache that all the clients will always see success,
 * despite accessing the same topic without a listener building a local projection (which would be required in a real
 * deployment).
 * The key result being measured is the average number of microseconds for a single message to commit (which is lower
 * the more messages are sent).
 * 
 * This requires env var to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 * 
 * Set VERBOSE to output per-run stats.
 */
public class PerfBalanceValidation {
	private static final String TEST_NAME = "AccountBalanceValidation";
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
	/**
	 * For this test, we always want it to pass, so the cache must be big enough for each client's sender and receiver.
	 */
	private static final short CACHE_SIZE = (short)(2 * CLIENTS[CLIENTS.length - 1]);
	/**
	 * The zero account can issue starting currency.
	 */
	private static final byte[] BANK_ACCOUNT = _createAccountKey(0);
	/**
	 * Transfer amount doesn't matter but we will make it more than 1 just so it is a different value.
	 */
	private static final int VALUE = 10;

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
		byte[] code = ContractPackager.createJarForClass(AccountBalanceValidation.class);
		byte[] arguments = ByteBuffer.allocate(Short.BYTES).putShort(CACHE_SIZE).array();
		
		// Create the components for the test run.
		Engine.TopicConfiguration topicToCreate = new Engine.TopicConfiguration(topic, code, arguments);
		Runner[] clients = new Runner[clientCount];
		for (int i = 0; i < clientCount; ++i) {
			clients[i] = new Runner(topic, messagesPerClient, (i + 1) * 2);
		}
		
		// Execute performance run.
		long[] nanoTimes = Engine.runWithUnitsOnThreads(_folder.newFolder(), TEST_NAME, nodeCount, messagesPerClient, new Engine.TopicConfiguration[] {topicToCreate}, clients);
		
		// Collect the maximum value, since we want to treat the block of clients as 1 data point.
		// If any of these were -1L, there was an error.
		long totalTimeNanos = 0L;
		for (long time : nanoTimes) {
			Assert.assertNotEquals(-1L, time);
			totalTimeNanos = Long.max(totalTimeNanos, time);
		}
		long perMessageTimeMicros = (totalTimeNanos / (clientCount * messagesPerClient)) / 1_000;
		
		if (null != System.getenv("VERBOSE")) {
			System.out.println("PERF (" + TEST_NAME + "):");
			System.out.println("\tNode count: " + nodeCount);
			System.out.println("\tClient count: " + clientCount);
			System.out.println("\tMessages per client: " + messagesPerClient);
			System.out.println("\tTotal time: " + (totalTimeNanos / 1_000_000) + " ms");
			System.out.println("\tTime per message: " + perMessageTimeMicros + " us");
		}
		return perMessageTimeMicros;
	}

	private static byte[] _packagePut(byte[] destinationKey, int value) {
		// We are just measuring performance of the success path so we don't have the information required to reconstruct missing entries.
		long clientLastSeenMutationOffset = 0L;
		int lastSenderValue = 0;
		int lastDestinationValue = 0;
		return ByteBuffer.allocate(destinationKey.length + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES)
				.put(destinationKey)
				.putLong(clientLastSeenMutationOffset)
				.putInt(value)
				.putInt(lastSenderValue)
				.putInt(lastDestinationValue)
				.array();
	}

	private static byte[] _createAccountKey(int value) {
		// Due to blockchain-specific validations in the AVM API, keys need to be 32 bytes.
		byte[] key = new byte[32];
		key[0] = (byte)value;
		return key;
	}

	private static ClientResult _sendPut(ClientConnection client, TopicName topic, byte[] sender, byte[] destination, int value) {
		byte[] payload = _packagePut(destination, value);
		return client.sendPut(topic, sender, payload);
	}

	private static class Runner implements IPerformanceUnit {
		private final TopicName _topic;
		private final ClientResult[] _results;
		private final byte[] _senderAddress;
		private final byte[] _receiverAddress;
		public Runner(TopicName topic, int iterationsToCapture, int baseAddress) {
			_topic = topic;
			_results = new ClientResult[iterationsToCapture];
			_senderAddress = _createAccountKey(baseAddress);
			_receiverAddress = _createAccountKey(baseAddress + 1);
		}
		@Override
		public void startWithClient(ClientConnection client) throws Throwable {
			// Seed the 2 accounts we will be interacting with.
			// -we want this to pass so provide enough balance for each transfer.
			ClientResult sender = _sendPut(client, _topic, BANK_ACCOUNT, _senderAddress, VALUE * _results.length);
			// -we just want to put the destination in the cache so the value doesn't matter.
			ClientResult destination = _sendPut(client, _topic, BANK_ACCOUNT, _receiverAddress, VALUE);
			// -both must pass.
			Assert.assertEquals(CommitInfo.Effect.VALID, sender.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, destination.waitForCommitted().effect);
		}
		@Override
		public void runIteration(ClientConnection client, int iteration) throws Throwable {
			// Send the message and store result for later.
			_results[iteration] = _sendPut(client, _topic, _senderAddress, _receiverAddress, VALUE);
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
