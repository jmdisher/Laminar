package com.jeffdisher.laminar.contracts;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.ProcessWrapper;
import com.jeffdisher.laminar.ServerWrapper;
import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


/**
 * Tests the AccountBalanceValidation contract in a clustered context.
 * Note that this test is intended to be non-trivial, emulating the way that this contract would be used to solve race
 * conditions, in a production environment, so multiple client-listener pairs race to perform transfers, periodically
 * verifying that the totals are consistent.
 */
public class TestAccountBalanceValidationCluster {
	private static final int CLIENT_COUNT = 4;
	private static final short CACHE_SIZE = 3;

	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testRaceCondition() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testRaceCondition-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testRaceCondition-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Deploy the contract and pre-seed the accounts with the starting balance.
		int expectedBalance = CLIENT_COUNT - 1;
		byte[] bank = new byte[32];
		byte[][] clientKeys = new byte[CLIENT_COUNT][];
		for (int i = 0; i < CLIENT_COUNT; ++i) {
			clientKeys[i] = _createAccountKey(1 + i);
		}
		TopicName topic = TopicName.fromString("test");
		long endStartupOffset = 0L;
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			byte[] jar = ContractPackager.createJarForClass(AccountBalanceValidation.class);
			byte[] args = ByteBuffer.allocate(Short.BYTES).putShort(CACHE_SIZE).array();
			CommitInfo deployCommit = client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, deployCommit.effect);
			long commitOffset = deployCommit.mutationOffset;
			
			for (int i = 0; i < CLIENT_COUNT; ++i) {
				byte[] put = _packagePut(clientKeys[i], commitOffset + i, expectedBalance, 0, 0);
				CommitInfo info = client.sendPut(topic, bank, put).waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
				endStartupOffset = info.mutationOffset;
			}
		}
		
		// We can now begin the test.
		// -first, set up a listener we will use for verifying the balances at each sync point.
		ProjectingListener verifier = new ProjectingListener(followerAddress, topic, CLIENT_COUNT);
		verifier.start();
		verifier.verifyBalancesAtMutation(2 + CLIENT_COUNT, expectedBalance);
		
		Thread[] clients = new Thread[CLIENT_COUNT];
		BarrierContainer barrier = new BarrierContainer(CLIENT_COUNT, endStartupOffset, (expectedOffset) -> verifier.verifyBalancesAtMutation(expectedOffset, expectedBalance));
		for (int i = 0; i < CLIENT_COUNT; ++i) {
			int thisIndex = i;
			clients[thisIndex] = new Thread() {
				@Override
				public void run() {
					InetSocketAddress addressForListener = (0 == (thisIndex % 2))
							? leaderAddress
							: followerAddress;
					try {
						ProjectingListener localVerifier = new ProjectingListener(addressForListener, topic, CLIENT_COUNT);
						localVerifier.start();
						try (ClientConnection localClient = ClientConnection.open(leaderAddress)) {
							// Wait for all the clients to start at once.
							barrier.calibrate(0).await();
							
							// Send our transfers _from_ account i.
							int updateCount = 0;
							int failureCount = 0;
							long lastMutationOffset = 0L;
							for (int j = 0; j < CLIENT_COUNT; ++j) {
								if (j != thisIndex) {
									byte[] put = localVerifier.packagePut(clientKeys[thisIndex], clientKeys[j], 1);
									boolean keepTrying = true;
									while (keepTrying) {
										CommitInfo putInfo = localClient.sendPut(topic, clientKeys[thisIndex], put).waitForCommitted();
										lastMutationOffset = putInfo.mutationOffset;
										if (CommitInfo.Effect.VALID == putInfo.effect) {
											keepTrying = false;
											updateCount += 1;
										} else {
											put = localVerifier.packagePut(clientKeys[thisIndex], clientKeys[j], 1);
											failureCount += 1;
										}
									}
								}
							}
							// Wait for synchronization and balance verification.
							barrier.calibrate(lastMutationOffset).await();
							
							// Send out transfers _to_ account i.
							for (int j = 0; j < CLIENT_COUNT; ++j) {
								if (j != thisIndex) {
									byte[] put = localVerifier.packagePut(clientKeys[j], clientKeys[thisIndex], 1);
									boolean keepTrying = true;
									while (keepTrying) {
										CommitInfo putInfo = localClient.sendPut(topic, clientKeys[j], put).waitForCommitted();
										lastMutationOffset = putInfo.mutationOffset;
										if (CommitInfo.Effect.VALID == putInfo.effect) {
											keepTrying = false;
											updateCount += 1;
										} else {
											put = localVerifier.packagePut(clientKeys[j], clientKeys[thisIndex], 1);
											failureCount += 1;
										}
									}
								}
							}
							// Wait for synchronization and balance verification.
							barrier.calibrate(lastMutationOffset).await();
							System.out.println("Client " + thisIndex + " performed " + updateCount + " updates and " + failureCount + " failures");
						}
						localVerifier.close();
						localVerifier.join();
					} catch (BrokenBarrierException e) {
						Assert.fail(e.getMessage());
					} catch (InterruptedException e) {
						Assert.fail(e.getMessage());
					} catch (IOException e) {
						Assert.fail(e.getMessage());
					}
				}
			};
			clients[i].start();
		}
		
		// Stop everything.
		for (int i = 0; i < CLIENT_COUNT; ++i) {
			clients[i].join();
		}
		verifier.close();
		verifier.join();
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}


	private static void _runConfigBuilder(String[] mainArgs) throws Throwable {
		String jarPath = System.getenv("CONFIG_BUILDER_JAR");
		if (null == jarPath) {
			throw new IllegalArgumentException("Missing CONFIG_BUILDER_JAR env var");
		}
		if (!new File(jarPath).exists()) {
			throw new IllegalArgumentException("JAR \"" + jarPath + "\" doesn't exist");
		}
		
		// Start the processes.
		ProcessWrapper process = ProcessWrapper.startedJavaProcess("ConfigBuilder", jarPath, mainArgs);
		// We don't use any filters.
		process.startFiltering();
		Assert.assertEquals(0, process.waitForTermination());
	}

	private static byte[] _packagePut(byte[] destinationKey, long clientLastSeenMutationOffset, int value, int lastSenderValue, int lastDestinationValue) {
		return ByteBuffer.allocate(destinationKey.length + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES)
				.put(destinationKey)
				.putLong(clientLastSeenMutationOffset)
				.putInt(value)
				.putInt(lastSenderValue)
				.putInt(lastDestinationValue)
				.array();
	}

	private byte[] _createAccountKey(int value) {
		// Due to blockchain-specific validations in the AVM API, keys need to be 32 bytes.
		byte[] key = new byte[32];
		key[0] = (byte)value;
		return key;
	}


	private static class ProjectingListener extends Thread {
		private final int[] _balance;
		private long _lastOffset;
		private final ListenerConnection _listener;
		
		public ProjectingListener(InetSocketAddress server, TopicName topic, int accountCount) throws IOException {
			_balance = new int[accountCount + 1];
			_lastOffset = 0L;
			_listener = ListenerConnection.open(server, topic, 0L);
		}
		
		public synchronized byte[] packagePut(byte[] sourceKey, byte[] destinationKey, int value) {
			int sourceValue = _balance[sourceKey[0]];
			int destinationValue = _balance[destinationKey[0]];
			return _packagePut(destinationKey, _lastOffset, value, sourceValue, destinationValue);
		}
		
		public void close() throws IOException {
			_listener.close();
		}
		
		public synchronized void verifyBalancesAtMutation(long mutationOffset, int expectedBalance) {
			while (_lastOffset < mutationOffset) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				}
			}
			_balance[0] = expectedBalance;
			for (int i = 0; i < _balance.length; ++i) {
				Assert.assertEquals(expectedBalance, _balance[i]);
			}
		}
		
		@Override
		public void run() {
			try {
				Consequence record = _listener.pollForNextConsequence();
				while(null != record) {
					// If this is a PUT, decode it and update our projection.
					synchronized (this) {
						if (Consequence.Type.KEY_PUT == record.type) {
							byte[] accountKey = ((Payload_KeyPut)record.payload).key;
							byte index = accountKey[0];
							if (0 != index) {
								byte[] value = ((Payload_KeyPut)record.payload).value;
								int balance = ByteBuffer.wrap(value).getInt();
								_balance[index] = balance;
							} else {
								// Index 0 is for the TRANSFER value - it terminates every sequence of events for a mutation.
								_lastOffset = record.globalOffset;
								// This number is the only thing we wait on, so trigger it now.
								this.notifyAll();
							}
						}
					}
					record = _listener.pollForNextConsequence();
				}
			} catch (InterruptedException e) {
				Assert.fail(e.getMessage());
			}
		}
	}


	private static class BarrierContainer implements Runnable {
		private final CyclicBarrier _barrier;
		private long _expectedOffset;
		private final Consumer<Long> _verificationTarget;
		
		public BarrierContainer(int parties, long initialOffset, Consumer<Long> verificationTarget) {
			_barrier = new CyclicBarrier(parties, this);
			_expectedOffset = initialOffset;
			_verificationTarget = verificationTarget;
		}
		
		public synchronized CyclicBarrier calibrate(long offset) {
			_expectedOffset = Long.max(_expectedOffset, offset);
			return _barrier;
		}
		
		@Override
		public void run() {
			_verificationTarget.accept(_expectedOffset);
		}
	}
}
