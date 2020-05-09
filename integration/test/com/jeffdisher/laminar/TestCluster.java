package com.jeffdisher.laminar;

import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.utils.TestingHelpers;


/**
 * Integration tests of behaviour of a multi-node cluster.
 * These tests are mostly related to things like Config updates, replication, and fail-over.
 * They involve a lot of use of clients and listeners, as well, but assume that they generally work (fundamental testing
 * of those is in TestClientsAndListeners).
 */
public class TestCluster {
	/**
	 * In this test, create 2 clients and a listener connected to a single node.
	 * Both clients send a message, then 1 sends a config update, wait for received, then both send another message.
	 * By this point, the other node in the config isn't up yet so it can't commit so wait 100 ms and observe that
	 * nobody has received the config update and that the listener hasn't seen the later messages.
	 * Then, start the new node and wait for all commits.
	 * Wait for listener to observe all messages.
	 * Check that all clients and the listener did receive the config update.
	 * Note that the config update doesn't go to a topic so the listener will only receive it as an out-of-band meta-
	 * data update.
	 */
	@Test
	public void testConfigUpdate() throws Throwable {
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testConfigUpdate-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 4);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			ClientResult result1_1 = client1.sendTemp(new byte[] {1});
			ClientResult result2_1 = client2.sendTemp(new byte[] {2});
			
			// Send config update (wait for received to ensure one client got the initial config).
			result1_1.waitForReceived();
			result2_1.waitForReceived();
			ClusterConfig originalConfig = client1.getCurrentConfig();
			Assert.assertEquals(1, originalConfig.entries.length);
			ConfigEntry originalEntry = originalConfig.entries[0];
			// Pre-generate the UUID for the follower.
			UUID followerUuid = UUID.randomUUID();
			ConfigEntry newEntry = new ConfigEntry(followerUuid, new InetSocketAddress(originalEntry.cluster.getAddress(), originalEntry.cluster.getPort() + 1000), new InetSocketAddress(originalEntry.client.getAddress(), originalEntry.client.getPort() + 1000));
			ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, newEntry});
			configSender = client1.getClientId();
			configNonce = client1.getNextNonce();
			beforeListener.skipNonceCheck(configSender, configNonce);
			ClientResult updateResult = client1.sendUpdateConfig(newConfig);
			updateResult.waitForReceived();
			
			// Now send another from each client.
			ClientResult result1_2 = client1.sendTemp(new byte[] {1});
			ClientResult result2_2 = client2.sendTemp(new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// At this point, the cluster should be stalled since it is missing a node required to reach consensus in the new config.
			// Wait for 100ms to verify that we don't see the new config and there is no listener progress.
			Thread.sleep(100L);
			Assert.assertEquals(originalConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(originalConfig.entries.length, client2.getCurrentConfig().entries.length);
			// Wait for the listener to receive the events before checking for the config (since it might not have received anything, yet).
			Assert.assertEquals(2, beforeListener.waitForEventCount(2));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapperWithUuid("testConfigUpdate-FOLLOWER", followerUuid, 3003, 3002, new File("/tmp/laminar2"));
			
			// Wait for everything to commit and check that the update we got is the same as the one we send.
			updateResult.waitForCommitted();
			result1_2.waitForCommitted();
			result2_2.waitForCommitted();
			Assert.assertEquals(newConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(newConfig.entries.length, client2.getCurrentConfig().entries.length);
			Assert.assertEquals(0, wrapper2.stop());
		} finally {
			client1.close();
			client2.close();
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 4);
		afterListener.setName("After");
		afterListener.skipNonceCheck(configSender, configNonce);
		afterListener.start();
		
		// Wait for the listeners to consume all 4 real events we sent (the commit of the config update doesn't go to a topic so they won't see that), and then verify they got the update as out-of-band.
		beforeListener.waitForTerminate();
		afterListener.waitForTerminate();
		Assert.assertEquals(2, beforeListener.getCurrentConfig().entries.length);
		Assert.assertEquals(2, afterListener.getCurrentConfig().entries.length);
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testReconnectWhileWaitingForClusterCommit() throws Throwable {
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testReconnectWhileWaitingForClusterCommit-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 7);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			ClientResult result1_1 = client1.sendTemp(new byte[] {1});
			ClientResult result2_1 = client2.sendTemp(new byte[] {2});
			
			// Send config update (wait for received to ensure one client got the initial config).
			result1_1.waitForReceived();
			result2_1.waitForReceived();
			ClusterConfig originalConfig = client1.getCurrentConfig();
			Assert.assertEquals(1, originalConfig.entries.length);
			ConfigEntry originalEntry = originalConfig.entries[0];
			// Pre-generate the UUID for the follower.
			UUID followerUuid = UUID.randomUUID();
			ConfigEntry newEntry = new ConfigEntry(followerUuid, new InetSocketAddress(originalEntry.cluster.getAddress(), originalEntry.cluster.getPort() + 1000), new InetSocketAddress(originalEntry.client.getAddress(), originalEntry.client.getPort() + 1000));
			ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, newEntry});
			configSender = client1.getClientId();
			configNonce = client1.getNextNonce();
			beforeListener.skipNonceCheck(configSender, configNonce);
			ClientResult updateResult = client1.sendUpdateConfig(newConfig);
			updateResult.waitForReceived();
			
			// Now send another from each client so there is something queued up which can't be committed until the new node is online.
			ClientResult result1_2 = client1.sendTemp(new byte[] {1});
			ClientResult result2_2 = client2.sendTemp(new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// Now, send the poison and another couple messages.
			ClientResult poisonResult = client1.sendPoison(new byte[] {0});
			ClientResult result1_3 = client1.sendTemp(new byte[] {1});
			ClientResult result2_3 = client2.sendTemp(new byte[] {2});
			
			// Verify everything so far has been received.
			result1_2.waitForReceived();
			result2_2.waitForReceived();
			poisonResult.waitForReceived();
			result1_3.waitForReceived();
			result2_3.waitForReceived();
			
			// At this point, the cluster should be stalled since it is missing a node required to reach consensus in the new config.
			// Wait for 100ms to verify that we don't see the new config and there is no listener progress.
			Thread.sleep(100L);
			Assert.assertEquals(originalConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(originalConfig.entries.length, client2.getCurrentConfig().entries.length);
			// Wait for the listener to receive the events before checking for the config (since it might not have received anything, yet).
			Assert.assertEquals(2, beforeListener.waitForEventCount(2));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			// WARNING:  This way of starting 2 nodes in-process is a huge hack and a better approach will need to be found, soon (mostly the way we are interacting with STDIN).
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapperWithUuid("testReconnectWhileWaitingForClusterCommit-FOLLOWER", followerUuid, 3003, 3002, new File("/tmp/laminar2"));
			
			// Wait for everything to commit and check that the update we got is the same as the one we send.
			updateResult.waitForCommitted();
			result1_2.waitForCommitted();
			result2_2.waitForCommitted();
			Assert.assertEquals(newConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(newConfig.entries.length, client2.getCurrentConfig().entries.length);
			Assert.assertEquals(0, wrapper2.stop());
		} finally {
			client1.close();
			client2.close();
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 7);
		afterListener.setName("After");
		afterListener.skipNonceCheck(configSender, configNonce);
		afterListener.start();
		
		// Wait for the listeners to consume all 4 real events we sent (the commit of the config update doesn't go to a topic so they won't see that), and then verify they got the update as out-of-band.
		beforeListener.waitForTerminate();
		afterListener.waitForTerminate();
		Assert.assertEquals(2, beforeListener.getCurrentConfig().entries.length);
		Assert.assertEquals(2, afterListener.getCurrentConfig().entries.length);
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	/**
	 * Tests how a client is redirected from follower to leader and that a listener is still able to be connected to the
	 * follower.
	 */
	@Test
	public void testListenToFollower() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testListenToFollower-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testListenToFollower-FOLLOWER", 2005, 2004, new File("/tmp/laminar2"));
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		
		// Start the listeners.
		CaptureListener leaderListener = new CaptureListener(leaderClientAddress, 3);
		CaptureListener followerListener = new CaptureListener(followerClientAddress, 3);
		leaderListener.setName("Leader");
		followerListener.setName("Follower");
		leaderListener.start();
		followerListener.start();
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(leaderClientAddress);
		ClientConnection client2 = ClientConnection.open(followerClientAddress);
		
		try {
			// Capture the config
			client1.waitForConnection();
			client2.waitForConnection();
			ClusterConfig leaderInitial = client1.getCurrentConfig();
			ClusterConfig followerInitial = client2.getCurrentConfig();
			Assert.assertEquals(1, leaderInitial.entries.length);
			Assert.assertEquals(1, followerInitial.entries.length);
			ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {leaderInitial.entries[0], followerInitial.entries[0]});
			
			// Send the config on client1 (will make it the leader) and wait for it to commit.
			leaderListener.skipNonceCheck(client1.getClientId(), 1L);
			followerListener.skipNonceCheck(client1.getClientId(), 1L);
			ClientResult configResult = client1.sendUpdateConfig(config);
			configResult.waitForCommitted();
			
			// Now, send another message on client1 and 2 on client2.
			ClientResult client1_1 = client1.sendTemp(new byte[] {1});
			ClientResult client2_1 = client2.sendTemp(new byte[] {2});
			ClientResult client2_2 = client2.sendTemp(new byte[] {3});
			
			// We can wait for these to commit at once, to stress the ordering further.
			client1_1.waitForCommitted();
			client2_1.waitForCommitted();
			client2_2.waitForCommitted();
			
			// Finally, check that the listeners saw all the results.
			EventRecord[] leaderRecords = leaderListener.waitForTerminate();
			EventRecord[] followerRecords = followerListener.waitForTerminate();
			Assert.assertEquals(leaderRecords[0].globalOffset, followerRecords[0].globalOffset);
			Assert.assertEquals(leaderRecords[1].globalOffset, followerRecords[1].globalOffset);
			Assert.assertEquals(leaderRecords[2].globalOffset, followerRecords[2].globalOffset);
			Assert.assertEquals(leaderRecords[1].globalOffset, leaderRecords[0].globalOffset + 1);
			Assert.assertEquals(leaderRecords[2].globalOffset, leaderRecords[1].globalOffset + 1);
			
			// The listeners should also have seen the config update, even though they didn't both change target.
			Assert.assertEquals(2, leaderListener.getCurrentConfig().entries.length);
			Assert.assertEquals(2, followerListener.getCurrentConfig().entries.length);
		} finally {
			// Shut down.
			client1.close();
			client2.close();
			Assert.assertEquals(0, leader.stop());
			Assert.assertEquals(0, follower.stop());
		}
	}

	/**
	 * Tests that the leader still makes progress even though a minority of the nodes are offline.
	 */
	@Test
	public void testMajorityProgress() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testMajorityProgress-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testMajorityProgress-FOLLOWER", 2005, 2004, new File("/tmp/laminar2"));
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		ConfigEntry missingServer = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(InetAddress.getLocalHost(), 2007), new InetSocketAddress(InetAddress.getLocalHost(), 2006));
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(leaderClientAddress);
		ClientConnection client2 = ClientConnection.open(followerClientAddress);
		
		try {
			// Capture the config
			client1.waitForConnection();
			client2.waitForConnection();
			ClusterConfig leaderInitial = client1.getCurrentConfig();
			ClusterConfig followerInitial = client2.getCurrentConfig();
			Assert.assertEquals(1, leaderInitial.entries.length);
			Assert.assertEquals(1, followerInitial.entries.length);
			ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {leaderInitial.entries[0], followerInitial.entries[0], missingServer});
			
			// Send the config on client1 (will make it the leader) and wait for it to commit.
			ClientResult configResult = client1.sendUpdateConfig(config);
			configResult.waitForCommitted();
			
			// Now, send another message on client1 and 2 on client2.
			ClientResult client1_1 = client1.sendTemp(new byte[] {1});
			ClientResult client2_1 = client2.sendTemp(new byte[] {2});
			ClientResult client2_2 = client2.sendTemp(new byte[] {3});
			
			// We can wait for these to commit at once, to stress the ordering further.
			client1_1.waitForCommitted();
			client2_1.waitForCommitted();
			client2_2.waitForCommitted();
			
			// Make sure that the config is consistent.
			Assert.assertEquals(3, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(3, client2.getCurrentConfig().entries.length);
		} finally {
			// Shut down.
			client1.close();
			client2.close();
			Assert.assertEquals(0, leader.stop());
			Assert.assertEquals(0, follower.stop());
		}
	}

	/**
	 * Tests that we make progress even when POISON breaks our connections and a node swaps late in the run.
	 */
	@Test
	public void testPoisonClusterSwitch() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testPoisonClusterSwitch-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testPoisonClusterSwitch-FOLLOWER", 2005, 2004, new File("/tmp/laminar2"));
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		UUID follower2Uuid = UUID.randomUUID();
		ServerWrapper follower2 = null;
		ConfigEntry follower2Config = new ConfigEntry(follower2Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 2007), new InetSocketAddress(InetAddress.getLocalHost(), 2006));
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(leaderClientAddress);
		ClientConnection client2 = ClientConnection.open(followerClientAddress);
		
		try {
			// Capture the config
			client1.waitForConnection();
			client2.waitForConnection();
			ClusterConfig leaderInitial = client1.getCurrentConfig();
			ClusterConfig followerInitial = client2.getCurrentConfig();
			Assert.assertEquals(1, leaderInitial.entries.length);
			Assert.assertEquals(1, followerInitial.entries.length);
			ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {leaderInitial.entries[0], followerInitial.entries[0], follower2Config});
			
			// Send a normal message, the config update, the poison, and a normal message on client1.
			ClientResult client1_1 = client1.sendTemp(new byte[] {1});
			ClientResult configResult = client1.sendUpdateConfig(config);
			ClientResult client1_2 = client1.sendPoison(new byte[] {2});
			ClientResult client1_3 = client1.sendTemp(new byte[] {3});
			
			// Wait for them all to commit and then send a normal message on client2 and wait for it to commit.
			client1_1.waitForCommitted();
			configResult.waitForCommitted();
			client1_2.waitForCommitted();
			client1_3.waitForCommitted();
			ClientResult client2_1 = client2.sendTemp(new byte[] {1});
			client2_1.waitForCommitted();
			
			// Then, stop the existing follower, create another one.
			Assert.assertEquals(0, follower.stop());
			follower = null;
			follower2 = ServerWrapper.startedServerWrapperWithUuid("testPoisonClusterSwitch-FOLLOWER2", follower2Uuid, 2007, 2006, new File("/tmp/laminar3"));
			
			// Send poison from client2 and a normal message from each client, then wait for everything to commit.
			ClientResult client2_2 = client2.sendPoison(new byte[] {2});
			ClientResult client1_4 = client1.sendTemp(new byte[] {4});
			ClientResult client2_3 = client1.sendTemp(new byte[] {3});
			client2_2.waitForCommitted();
			client1_4.waitForCommitted();
			client2_3.waitForCommitted();
			
			// Make sure that the config is consistent.
			Assert.assertEquals(3, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(3, client2.getCurrentConfig().entries.length);
		} finally {
			// Shut down.
			client1.close();
			client2.close();
			Assert.assertEquals(0, leader.stop());
			if (null != follower) {
				Assert.assertEquals(0, follower.stop());
			}
			if (null != follower2) {
				Assert.assertEquals(0, follower2.stop());
			}
		}
	}

	/**
	 * Tests that we can force the leader to switch and verify that things still work.
	 * We use 1 client, 2 listeners, and 1 ad-hoc connection.  The client should redirect to the new leader after the
	 * force call is sent over the ad-hoc connection and the listeners should each continue listening to their
	 * respective servers.
	 */
	@Test
	public void testForceLeader() throws Throwable {
		UUID server1Uuid = UUID.randomUUID();
		UUID server2Uuid = UUID.randomUUID();
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testForceLeader-1", server1Uuid, 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress server1Address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testForceLeader-2", server2Uuid, 2005, 2004, new File("/tmp/laminar2"));
		InetSocketAddress server2Address= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		
		// Start the listeners.
		CaptureListener listener1 = new CaptureListener(server1Address, 3);
		CaptureListener listener2 = new CaptureListener(server2Address, 3);
		listener1.setName("1");
		listener2.setName("2");
		listener1.start();
		listener2.start();
		CaptureListener timingListener = new CaptureListener(server2Address, 1);
		timingListener.setName("timing");
		timingListener.start();
		
		ClientConnection client = ClientConnection.open(server1Address);
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(server1Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 2003), server1Address),
				new ConfigEntry(server2Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 2005), server2Address),
		});
		
		try {
			// Send the initial config to create the initial leader and wait for it to commit.
			listener1.skipNonceCheck(client.getClientId(), 1L);
			listener2.skipNonceCheck(client.getClientId(), 1L);
			timingListener.skipNonceCheck(client.getClientId(), 1L);
			ClientResult configResult = client.sendUpdateConfig(config);
			configResult.waitForCommitted();
			
			// Send the initial message and wait for it to commit (which means it has made it to both nodes).
			ClientResult result1 = client.sendTemp(new byte[] {1});
			result1.waitForCommitted();
			// Wait until it commits on the other, too.
			timingListener.waitForTerminate();
			
			// Now, create the ad-hoc message to send the FORCE_LEADER.
			try (Socket adhoc = new Socket(server2Address.getAddress(), server2Address.getPort())) {
				OutputStream toServer = adhoc.getOutputStream();
				TestingHelpers.writeMessageInFrame(toServer, ClientMessage.forceLeader().serialize());
				// Read until disconnect.
				adhoc.getInputStream().read();
			}
			
			ClientConnection client2 = ClientConnection.open(server2Address);
			client2.sendTemp(new byte[] {2}).waitForCommitted();
			Assert.assertEquals(server2Address, client2.getCurrentServer());
			client2.close();
			
			// Send the other message
			ClientResult result2 = client.sendTemp(new byte[] {3});
			result2.waitForCommitted();
			
			// Finally, check that the listeners saw all the results.
			EventRecord[] records1 = listener1.waitForTerminate();
			EventRecord[] records2 = listener2.waitForTerminate();
			Assert.assertEquals(records1[0].globalOffset, records2[0].globalOffset);
			Assert.assertEquals(records1[1].globalOffset, records2[1].globalOffset);
			Assert.assertEquals(records1[1].globalOffset, records2[0].globalOffset + 1);
			Assert.assertEquals(records1[2].globalOffset, records2[2].globalOffset);
			Assert.assertEquals(records1[2].globalOffset, records2[1].globalOffset + 1);
		} finally {
			// Shut down.
			client.close();
			Assert.assertEquals(0, server1.stop());
			Assert.assertEquals(0, server2.stop());
		}
	}

	/**
	 * Stress tests reconnection to a server by forcing the connection to close while many messages are in-flight.
	 */
	@Test
	public void testReconnectStress() throws Throwable {
		ServerWrapper server = ServerWrapper.startedServerWrapper("testReconnectStress", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		
		ClientConnection client = ClientConnection.open(serverAddress);
		try {
			client.sendTemp(new byte[] {-1}).waitForCommitted();
			// Send lots of messages and then force a reconnect, before we have blocked on any of them.
			ClientResult[] results = new ClientResult[100];
			for (int i = 0; i < 50; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			// We can't force reconnect until the connection appears.
			client.waitForConnection();
			client.forceReconnect();
			for (int i = 50; i < results.length; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForCommitted();
			}
			client.forceReconnect();
			ClientResult[] after = new ClientResult[10];
			for (int i = 0; i < after.length; ++i) {
				after[i] = client.sendTemp(new byte[] {(byte)i});
			}
			for (int i = 0; i < after.length; ++i) {
				after[i].waitForCommitted();
			}
		} finally {
			// Shut down.
			client.close();
			Assert.assertEquals(0, server.stop());
		}
	}

	/**
	 * Creates a 5-node cluster to make sure that larger clusters still operate correctly (since most of the tests are
	 * on specific behaviours on a smaller scale, this is a common-case on a large scale).
	 * At the end, listens to each node to verify it has received all messages.
	 */
	@Test
	public void testLargeCluster() throws Throwable {
		// Create our config.
		InetSocketAddress server1Address = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		InetSocketAddress server2Address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		InetSocketAddress server3Address = new InetSocketAddress(InetAddress.getLocalHost(), 2003);
		InetSocketAddress server4Address = new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		InetSocketAddress server5Address = new InetSocketAddress(InetAddress.getLocalHost(), 2005);
		UUID server1Uuid = UUID.randomUUID();
		UUID server2Uuid = UUID.randomUUID();
		UUID server3Uuid = UUID.randomUUID();
		UUID server4Uuid = UUID.randomUUID();
		UUID server5Uuid = UUID.randomUUID();
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(server1Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3001), server1Address),
				new ConfigEntry(server2Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3002), server2Address),
				new ConfigEntry(server3Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3003), server3Address),
				new ConfigEntry(server4Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3004), server4Address),
				new ConfigEntry(server5Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3005), server5Address),
		});
		
		// We want to ramp up slowly so start with 3 servers (so we have consensus) and bring on the other 2 as we go, and then shut down 2.
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-1", server1Uuid, 3001, 2001, new File("/tmp/laminar1"));
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-2", server2Uuid, 3002, 2002, new File("/tmp/laminar2"));
		ServerWrapper server3 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-3", server3Uuid, 3003, 2003, new File("/tmp/laminar3"));
		ServerWrapper server4 = null;
		ServerWrapper server5 = null;
		
		// Start the client, set the config, and run the test.
		ClientConnection client = ClientConnection.open(server1Address);
		try {
			client.sendUpdateConfig(config);
			
			// We want to send the messages in bursts as we bring more servers online.
			_runBatch(client, 10, 0);
			server4 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-4", server4Uuid, 3004, 2004, new File("/tmp/laminar4"));
			_runBatch(client, 10, 10);
			server5 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-5", server4Uuid, 3005, 2005, new File("/tmp/laminar5"));
			_runBatch(client, 10, 20);
			Assert.assertEquals(0, server2.stop());
			server2 = null;
			_runBatch(client, 10, 30);
			Assert.assertEquals(0, server3.stop());
			server3 = null;
			
			// Start a listener on each remaining server and verify we see all 40 mutations.
			EventRecord[] records1 = _listenOnServer(server1Address, client.getClientId(), 40);
			EventRecord[] records4 = _listenOnServer(server4Address, client.getClientId(), 40);
			EventRecord[] records5 = _listenOnServer(server5Address, client.getClientId(), 40);
			for (int i = 0; i < 40; ++i) {
				Assert.assertEquals((byte)i, records1[i].payload[0]);
				Assert.assertEquals((byte)i, records4[i].payload[0]);
				Assert.assertEquals((byte)i, records5[i].payload[0]);
			}
		} finally {
			// Shut down.
			client.close();
			Assert.assertEquals(0, server1.stop());
			if (null != server2) {
				Assert.assertEquals(0, server2.stop());
			}
			if (null != server3) {
				Assert.assertEquals(0, server3.stop());
			}
			if (null != server4) {
				Assert.assertEquals(0, server4.stop());
			}
			if (null != server5) {
				Assert.assertEquals(0, server5.stop());
			}
		}
	}

	/**
	 * Creates a 5-node cluster and continuously stops the restarts the leader to force elections.
	 * At the end, listens to each node to verify it has received all messages.
	 */
	@Test
	public void testElectionTimeout() throws Throwable {
		// Create our config.
		InetSocketAddress server1Address = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		InetSocketAddress server2Address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		InetSocketAddress server3Address = new InetSocketAddress(InetAddress.getLocalHost(), 2003);
		InetSocketAddress server4Address = new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		InetSocketAddress server5Address = new InetSocketAddress(InetAddress.getLocalHost(), 2005);
		UUID serverUuids[] = new UUID[] {
				UUID.randomUUID(),
				UUID.randomUUID(),
				UUID.randomUUID(),
				UUID.randomUUID(),
				UUID.randomUUID(),
		};
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(serverUuids[0], new InetSocketAddress(InetAddress.getLocalHost(), 3001), server1Address),
				new ConfigEntry(serverUuids[1], new InetSocketAddress(InetAddress.getLocalHost(), 3002), server2Address),
				new ConfigEntry(serverUuids[2], new InetSocketAddress(InetAddress.getLocalHost(), 3003), server3Address),
				new ConfigEntry(serverUuids[3], new InetSocketAddress(InetAddress.getLocalHost(), 3004), server4Address),
				new ConfigEntry(serverUuids[4], new InetSocketAddress(InetAddress.getLocalHost(), 3005), server5Address),
		});
		
		// Start all 5 servers since we will rotate out a single one at each step.
		ServerWrapper servers[] = new ServerWrapper[5];
		for (int i = 0; i < servers.length; ++i) {
			servers[i] = _startServerWrapper("testElectionTimeout", serverUuids[i], i);
		}
		
		// Start the client, set the config, and run the test.
		ClientConnection client = ClientConnection.open(server1Address);
		try {
			client.sendUpdateConfig(config);
			
			// We want to send the messages in bursts as we bring more servers online.
			_runBatch(client, 10, 0);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, 10, 10);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, 10, 20);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, 10, 30);
			
			EventRecord[] records1 = _listenOnServer(server1Address, client.getClientId(), 40);
			EventRecord[] records2 = _listenOnServer(server2Address, client.getClientId(), 40);
			EventRecord[] records3 = _listenOnServer(server3Address, client.getClientId(), 40);
			EventRecord[] records4 = _listenOnServer(server4Address, client.getClientId(), 40);
			EventRecord[] records5 = _listenOnServer(server5Address, client.getClientId(), 40);
			for (int i = 0; i < 40; ++i) {
				Assert.assertEquals((byte)i, records1[i].payload[0]);
				Assert.assertEquals((byte)i, records2[i].payload[0]);
				Assert.assertEquals((byte)i, records3[i].payload[0]);
				Assert.assertEquals((byte)i, records4[i].payload[0]);
				Assert.assertEquals((byte)i, records5[i].payload[0]);
			}
		} finally {
			// Shut down.
			client.close();
			for (ServerWrapper wrapper: servers) {
				Assert.assertEquals(0, wrapper.stop());
			}
		}
	}


	private EventRecord[] _listenOnServer(InetSocketAddress serverAddress, UUID clientUuid, int count) throws Throwable {
		CaptureListener listener = new CaptureListener(serverAddress, count);
		listener.skipNonceCheck(clientUuid, 1L);
		listener.start();
		return listener.waitForTerminate();
	}

	private void _runBatch(ClientConnection client, int size, int bias) throws Throwable {
		ClientResult results[] = new ClientResult[size];
		for (int i = 0; i < results.length; ++i) {
			results[i] = client.sendTemp(new byte[] {(byte)(i + bias)});
		}
		for (int i = 0; i < results.length; ++i) {
			results[i].waitForCommitted();
		}
	}

	private void _rotateServer(UUID[] serverUuids, ClusterConfig config, ServerWrapper[] servers,
			ClientConnection client) throws InterruptedException, Throwable {
		int attachedIndex = _getAttachedServerIndex(config, client);
		Assert.assertEquals(0, servers[attachedIndex].stop());
		servers[attachedIndex] = _startServerWrapper("testElectionTimeout", serverUuids[attachedIndex], attachedIndex);
	}

	private int _getAttachedServerIndex(ClusterConfig config, ClientConnection client) {
		int attachedIndex = -1;
		for (int i = 0; i < config.entries.length; ++i) {
			ConfigEntry entry = config.entries[i];
			if (entry.client.equals(client.getCurrentServer())) {
				attachedIndex = i;
				break;
			}
		}
		Assert.assertNotEquals(-1, attachedIndex);
		return attachedIndex;
	}

	private ServerWrapper _startServerWrapper(String name, UUID uuid, int i) throws Throwable {
		int count = i + 1;
		return ServerWrapper.startedServerWrapperWithUuid(name + "-" + count, uuid, 3000 + count, 2000 + count, new File("/tmp/laminar" + count));
	}
}
