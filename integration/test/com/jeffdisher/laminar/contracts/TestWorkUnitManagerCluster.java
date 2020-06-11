package com.jeffdisher.laminar.contracts;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
 * Tests the WorkUnitManager contract in a clustered context.
 * This is a contrived contract so the test is mostly just to demonstrate that the distribution does work correctly.
 * Several clients are created and race for the next work unit up to a fixed number of units.  Upon acquiring a unit,
 * the client will sleep for some time in order to emulate performing the work.  At the end, each client prints out how
 * many units they completed in order to provide a sense of the "fairness" of the distribution.
 */
public class TestWorkUnitManagerCluster {
	private static final int CLIENT_COUNT = 5;
	private static final int TOTAL_WORK = 400;
	private static final long MILLIS_PER_UNIT = 2L;

	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testParallelWork() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testParallelWork-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testParallelWork-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Deploy the program for the new topic.
		TopicName topic = TopicName.fromString("test");
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			byte[] jar = ContractPackager.createJarForClass(WorkUnitManager.class);
			byte[] args = new byte[0];
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
		}
		
		// Create the registry.
		WorkRegistry registry = new WorkRegistry();
		// Create all the clients, gated on a barrier.
		CyclicBarrier startBarrier = new CyclicBarrier(CLIENT_COUNT);
		Worker[] workers = new Worker[CLIENT_COUNT];
		for (int i = 0; i < CLIENT_COUNT; ++i) {
			InetSocketAddress targetServer = (0 == (i % 2))
					? leaderAddress
					: followerAddress;
			workers[i] = new Worker(registry, targetServer, topic, startBarrier);
			// We can start these, immediately, since the block on the barrier.
			workers[i].start();
		}
		
		// Stop all the clients.
		for (int i = 0; i < CLIENT_COUNT; ++i) {
			workers[i].join();
		}
		
		// Verify all the work was consumed.
		registry.verify();
		
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


	private static class WorkRegistry {
		private final boolean[] _doneWork = new boolean[TOTAL_WORK];
		
		public void handleUnit(int unit) {
			synchronized (this) {
				Assert.assertFalse(_doneWork[unit-1]);
				_doneWork[unit-1] = true;
			}
			try {
				Thread.sleep(MILLIS_PER_UNIT);
			} catch (InterruptedException e) {
				Assert.fail(e.getMessage());
			}
		}
		
		public void verify() {
			for (boolean flag : _doneWork) {
				Assert.assertTrue(flag);
			}
		}
	}


	private static class Worker {
		private final Thread _clientThread;
		private final Thread _listenerThread;
		private volatile int _nextUnitToTry;
		
		public Worker(WorkRegistry registry, InetSocketAddress server, TopicName topic, CyclicBarrier startBarrier) throws IOException {
			_clientThread = new Thread(() -> {
				int successCount = 0;
				int failCount = 0;
				// If this server isn't leader we will be redirected.
				try (ClientConnection client = ClientConnection.open(server)) {
					client.waitForConnection();
					startBarrier.await();
					int unit = 1;
					byte[] key = new byte[32];
					while (unit <= TOTAL_WORK) {
						// Try to get this unit.
						if (CommitInfo.Effect.VALID == client.sendPut(topic, key, ByteBuffer.allocate(Integer.BYTES).putInt(unit).array()).waitForCommitted().effect) {
							// We won this unit so consume it.
							registry.handleUnit(unit);
							successCount += 1;
						} else {
							failCount += 1;
						}
						// Whether we consumed it, or not, see which one is next.
						unit = waitForNextUnit(unit);
					}
				} catch (IOException e) {
					Assert.fail(e.getMessage());
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				} catch (BrokenBarrierException e) {
					Assert.fail(e.getMessage());
				}
				System.out.println("Client handled units: " + successCount + " failed: " + failCount);
			});
			_listenerThread = new Thread(() -> {
				try (ListenerConnection listener = ListenerConnection.open(server, topic, 0L)) {
					int lastUnitObserved = 0;
					while (lastUnitObserved < TOTAL_WORK) {
						Consequence record = listener.pollForNextConsequence();
						if (Consequence.Type.KEY_PUT == record.type) {
							lastUnitObserved = ByteBuffer.wrap(((Payload_KeyPut) record.payload).value).getInt();
							// Set the next unit to be the one after what we just observed.
							putNextUnit(lastUnitObserved + 1);
						}
					}
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				} catch (IOException e) {
					Assert.fail(e.getMessage());
				}
			});
			_nextUnitToTry = 1;
		}
		
		public void start() {
			_clientThread.start();
			_listenerThread.start();
		}
		
		public void join() throws InterruptedException {
			_clientThread.join();
			_listenerThread.join();
		}
		
		public synchronized void putNextUnit(int nextUnit) {
			_nextUnitToTry = nextUnit;
			this.notifyAll();
		}
		
		public synchronized int waitForNextUnit(int currentUnit) {
			// Wait until we see someone at least consume what we were just trying.
			while (currentUnit >= _nextUnitToTry) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				}
			}
			return _nextUnitToTry;
		}
	}
}
