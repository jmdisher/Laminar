package com.jeffdisher.laminar;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.TestingHelpers;


/**
 * Integration tests of behaviour of a multi-node cluster.
 * These tests are mostly related to things like Config updates, replication, and fail-over.
 * They involve a lot of use of clients and listeners, as well, but assume that they generally work (fundamental testing
 * of those is in TestClientsAndListeners).
 */
public class TestCluster {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testConfigUpdate-LEADER", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, topic, 4);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			Assert.assertEquals(CommitInfo.Effect.VALID, client1.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result1_1 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult result2_1 = client2.sendPut(topic, new byte[0], new byte[] {2});
			
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
			ClientResult result1_2 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult result2_2 = client2.sendPut(topic, new byte[0], new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// At this point, the cluster should be stalled since it is missing a node required to reach consensus in the new config.
			// Wait for 100ms to verify that we don't see the new config and there is no listener progress.
			Thread.sleep(100L);
			Assert.assertEquals(originalConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(originalConfig.entries.length, client2.getCurrentConfig().entries.length);
			// Wait for the listener to receive the events before checking for the config (since it might not have received anything, yet).
			Assert.assertEquals(3, beforeListener.waitForEventCount(3));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapperWithUuid("testConfigUpdate-FOLLOWER", followerUuid, 3003, 3002, _folder.newFolder());
			
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
		CaptureListener afterListener = new CaptureListener(address, topic, 4);
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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testReconnectWhileWaitingForClusterCommit-LEADER", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, topic, 7);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			Assert.assertEquals(CommitInfo.Effect.VALID, client1.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result1_1 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult result2_1 = client2.sendPut(topic, new byte[0], new byte[] {2});
			
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
			ClientResult result1_2 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult result2_2 = client2.sendPut(topic, new byte[0], new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// Now, send the poison and another couple messages.
			ClientResult poisonResult = client1.sendPoison(topic, new byte[0], new byte[] {0});
			ClientResult result1_3 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult result2_3 = client2.sendPut(topic, new byte[0], new byte[] {2});
			
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
			Assert.assertEquals(3, beforeListener.waitForEventCount(3));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			// WARNING:  This way of starting 2 nodes in-process is a huge hack and a better approach will need to be found, soon (mostly the way we are interacting with STDIN).
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapperWithUuid("testReconnectWhileWaitingForClusterCommit-FOLLOWER", followerUuid, 3003, 3002, _folder.newFolder());
			
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
		CaptureListener afterListener = new CaptureListener(address, topic, 7);
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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testListenToFollower-LEADER", 2003, 2002, _folder.newFolder());
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testListenToFollower-FOLLOWER", 2005, 2004, _folder.newFolder());
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		
		// Start the listeners.
		CaptureListener leaderListener = new CaptureListener(leaderClientAddress, topic, 3);
		CaptureListener followerListener = new CaptureListener(followerClientAddress, topic, 3);
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
			Assert.assertEquals(CommitInfo.Effect.VALID, client1.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult client1_1 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult client2_1 = client2.sendPut(topic, new byte[0], new byte[] {2});
			ClientResult client2_2 = client2.sendPut(topic, new byte[0], new byte[] {3});
			
			// We can wait for these to commit at once, to stress the ordering further.
			client1_1.waitForCommitted();
			client2_1.waitForCommitted();
			client2_2.waitForCommitted();
			
			// Finally, check that the listeners saw all the results.
			Consequence[] leaderRecords = leaderListener.waitForTerminate();
			Consequence[] followerRecords = followerListener.waitForTerminate();
			Assert.assertEquals(leaderRecords[0], followerRecords[0]);
			Assert.assertEquals(leaderRecords[1], followerRecords[1]);
			Assert.assertEquals(leaderRecords[2], followerRecords[2]);
			Assert.assertEquals(leaderRecords[1].intentionOffset, leaderRecords[0].intentionOffset + 1);
			Assert.assertEquals(leaderRecords[2].intentionOffset, leaderRecords[1].intentionOffset + 1);
			
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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testMajorityProgress-LEADER", 2003, 2002, _folder.newFolder());
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testMajorityProgress-FOLLOWER", 2005, 2004, _folder.newFolder());
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
			Assert.assertEquals(CommitInfo.Effect.VALID, client1.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult client1_1 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult client2_1 = client2.sendPut(topic, new byte[0], new byte[] {2});
			ClientResult client2_2 = client2.sendPut(topic, new byte[0], new byte[] {3});
			
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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testPoisonClusterSwitch-LEADER", 2003, 2002, _folder.newFolder());
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testPoisonClusterSwitch-FOLLOWER", 2005, 2004, _folder.newFolder());
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
			Assert.assertEquals(CommitInfo.Effect.VALID, client1.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult client1_1 = client1.sendPut(topic, new byte[0], new byte[] {1});
			ClientResult configResult = client1.sendUpdateConfig(config);
			ClientResult client1_2 = client1.sendPoison(topic, new byte[0], new byte[] {2});
			ClientResult client1_3 = client1.sendPut(topic, new byte[0], new byte[] {3});
			
			// Wait for them all to commit and then send a normal message on client2 and wait for it to commit.
			client1_1.waitForCommitted();
			configResult.waitForCommitted();
			client1_2.waitForCommitted();
			client1_3.waitForCommitted();
			ClientResult client2_1 = client2.sendPut(topic, new byte[0], new byte[] {1});
			client2_1.waitForCommitted();
			
			// Then, stop the existing follower, create another one.
			Assert.assertEquals(0, follower.stop());
			follower = null;
			follower2 = ServerWrapper.startedServerWrapperWithUuid("testPoisonClusterSwitch-FOLLOWER2", follower2Uuid, 2007, 2006, _folder.newFolder());
			// Interestingly, delaying the client's attempt to reconnect by 100 ms makes a racy message read in the ClusterManager more likely.
			Thread.sleep(100);
			
			// Send poison from client2 and a normal message from each client, then wait for everything to commit.
			ClientResult client2_2 = client2.sendPoison(topic, new byte[0], new byte[] {2});
			ClientResult client1_4 = client1.sendPut(topic, new byte[0], new byte[] {4});
			ClientResult client2_3 = client1.sendPut(topic, new byte[0], new byte[] {3});
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
		TopicName topic = TopicName.fromString("test");
		UUID server1Uuid = UUID.randomUUID();
		UUID server2Uuid = UUID.randomUUID();
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testForceLeader-1", server1Uuid, 2003, 2002, _folder.newFolder());
		InetSocketAddress server1Address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testForceLeader-2", server2Uuid, 2005, 2004, _folder.newFolder());
		InetSocketAddress server2Address= new InetSocketAddress(InetAddress.getLocalHost(), 2004);
		
		// Start the listeners.
		CaptureListener listener1 = new CaptureListener(server1Address, topic, 3);
		CaptureListener listener2 = new CaptureListener(server2Address, topic, 3);
		listener1.setName("1");
		listener2.setName("2");
		listener1.start();
		listener2.start();
		CaptureListener timingListener = new CaptureListener(server2Address, topic, 1);
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
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result1 = client.sendPut(topic, new byte[0], new byte[] {1});
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
			client2.sendPut(topic, new byte[0], new byte[] {2}).waitForCommitted();
			Assert.assertEquals(server2Address, client2.getCurrentServer());
			client2.close();
			
			// Send the other message
			ClientResult result2 = client.sendPut(topic, new byte[0], new byte[] {3});
			result2.waitForCommitted();
			
			// Finally, check that the listeners saw all the results.
			Consequence[] records1 = listener1.waitForTerminate();
			Consequence[] records2 = listener2.waitForTerminate();
			Assert.assertEquals(records1[0], records2[0]);
			Assert.assertEquals(records1[1], records2[1]);
			Assert.assertEquals(records1[1].intentionOffset, records2[0].intentionOffset + 1);
			Assert.assertEquals(records1[2], records2[2]);
			Assert.assertEquals(records1[2].intentionOffset, records2[1].intentionOffset + 1);
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
		TopicName topic = TopicName.fromString("test");
		ServerWrapper server = ServerWrapper.startedServerWrapper("testReconnectStress", 2003, 2002, _folder.newFolder());
		InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		
		ClientConnection client = ClientConnection.open(serverAddress);
		try {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			client.sendPut(topic, new byte[0], new byte[] {-1}).waitForCommitted();
			// Send lots of messages and then force a reconnect, before we have blocked on any of them.
			ClientResult[] results = new ClientResult[100];
			for (int i = 0; i < 50; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			// We can't force reconnect until the connection appears.
			client.waitForConnection();
			client.forceReconnect();
			for (int i = 50; i < results.length; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForCommitted();
			}
			client.forceReconnect();
			ClientResult[] after = new ClientResult[10];
			for (int i = 0; i < after.length; ++i) {
				after[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
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
		TopicName topic = TopicName.fromString("test");
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
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-1", server1Uuid, 3001, 2001, _folder.newFolder());
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-2", server2Uuid, 3002, 2002, _folder.newFolder());
		ServerWrapper server3 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-3", server3Uuid, 3003, 2003, _folder.newFolder());
		ServerWrapper server4 = null;
		ServerWrapper server5 = null;
		
		// Start the client, set the config, and run the test.
		ClientConnection client = ClientConnection.open(server1Address);
		try {
			client.sendUpdateConfig(config);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			
			// We want to send the messages in bursts as we bring more servers online.
			_runBatch(client, topic, 10, 0);
			server4 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-4", server4Uuid, 3004, 2004, _folder.newFolder());
			_runBatch(client, topic, 10, 10);
			server5 = ServerWrapper.startedServerWrapperWithUuid("testLargeCluster-5", server4Uuid, 3005, 2005, _folder.newFolder());
			_runBatch(client, topic, 10, 20);
			Assert.assertEquals(0, server2.stop());
			server2 = null;
			_runBatch(client, topic, 10, 30);
			Assert.assertEquals(0, server3.stop());
			server3 = null;
			
			// Start a listener on each remaining server and verify we see all 41 mutations (but want to skip the first, since it is just the topic creation).
			Consequence[] records1 = _listenOnServer(server1Address, topic, client.getClientId(), 41);
			Consequence[] records4 = _listenOnServer(server4Address, topic, client.getClientId(), 41);
			Consequence[] records5 = _listenOnServer(server5Address, topic, client.getClientId(), 41);
			for (int i = 0; i < 40; ++i) {
				// Skip the topic creation.
				int index = i + 1;
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records1[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records4[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records5[index].payload).value[0]);
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
		TopicName topic = TopicName.fromString("test");
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
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			
			// We want to send the messages in bursts as we bring more servers online.
			_runBatch(client, topic, 10, 0);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, topic, 10, 10);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, topic, 10, 20);
			_rotateServer(serverUuids, config, servers, client);
			_runBatch(client, topic, 10, 30);
			
			// We need to add an extra event and skip over it to account for the creation of the topic.
			Consequence[] records1 = _listenOnServer(server1Address, topic, client.getClientId(), 41);
			Consequence[] records2 = _listenOnServer(server2Address, topic, client.getClientId(), 41);
			Consequence[] records3 = _listenOnServer(server3Address, topic, client.getClientId(), 41);
			Consequence[] records4 = _listenOnServer(server4Address, topic, client.getClientId(), 41);
			Consequence[] records5 = _listenOnServer(server5Address, topic, client.getClientId(), 41);
			for (int i = 0; i < 40; ++i) {
				int index = i + 1;
				
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records1[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records2[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records3[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records4[index].payload).value[0]);
				Assert.assertEquals((byte)i, ((Payload_KeyPut)records5[index].payload).value[0]);
			}
		} finally {
			// Shut down.
			client.close();
			for (ServerWrapper wrapper: servers) {
				Assert.assertEquals(0, wrapper.stop());
			}
		}
	}

	/**
	 * Tests that we observe the expected CommitInfo.Effect from create/destroy topic messages.
	 */
	@Test
	public void testCreateDestroyTopic() throws Throwable {
		TopicName implicit = TopicName.fromString("implicit");
		TopicName explicit = TopicName.fromString("explicit");
		UUID leaderUuid = UUID.randomUUID();
		ServerWrapper leader = ServerWrapper.startedServerWrapperWithUuid("testCreateDestroyTopic-LEADER", leaderUuid, 3001, 2001, _folder.newFolder());
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		UUID followerUuid = UUID.randomUUID();
		ServerWrapper follower = ServerWrapper.startedServerWrapperWithUuid("testCreateDestroyTopic-FOLLOWER", followerUuid, 3002, 2002, _folder.newFolder());
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try(ClientConnection client = ClientConnection.open(leaderClientAddress)) {
			ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
					new ConfigEntry(leaderUuid, new InetSocketAddress(InetAddress.getLocalHost(), 3001), leaderClientAddress),
					new ConfigEntry(followerUuid, new InetSocketAddress(InetAddress.getLocalHost(), 3002), followerClientAddress),
			});
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendUpdateConfig(config).waitForCommitted().effect);
			
			// Send a message to the implicit topic and verify attempting to create it fails but we can destroy it.
			ClientResult precreate1 = client.sendCreateTopic(implicit);
			Assert.assertEquals(CommitInfo.Effect.VALID, precreate1.waitForCommitted().effect);
			ClientResult result1 = client.sendPut(implicit, new byte[0], new byte[] {1});
			ClientResult result2 = client.sendCreateTopic(implicit);
			Assert.assertEquals(CommitInfo.Effect.VALID, result1.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.INVALID, result2.waitForCommitted().effect);
			ClientResult result3 = client.sendDestroyTopic(implicit);
			Assert.assertEquals(CommitInfo.Effect.VALID, result3.waitForCommitted().effect);
			
			// Explicitly create a new topic, send a message to both it and the invalid, then destroy both.
			ClientResult precreate2 = client.sendCreateTopic(implicit);
			Assert.assertEquals(CommitInfo.Effect.VALID, precreate2.waitForCommitted().effect);
			ClientResult result4 = client.sendCreateTopic(explicit);
			ClientResult result5 = client.sendPut(implicit, new byte[0], new byte[] {2});
			ClientResult result6 = client.sendPut(explicit, new byte[0], new byte[] {3});
			ClientResult result7 = client.sendDestroyTopic(implicit);
			ClientResult result8 = client.sendDestroyTopic(explicit);
			Assert.assertEquals(CommitInfo.Effect.VALID, result4.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, result5.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, result6.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, result7.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, result8.waitForCommitted().effect);
			
			// Now, attach a listener to each server and ensure that we observe all the VALID messages.
			try (ListenerConnection leaderImplicit = ListenerConnection.open(leaderClientAddress, implicit, 0L)) {
				Assert.assertEquals(precreate1.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result1.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result3.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(precreate2.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result5.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result7.message.nonce, leaderImplicit.pollForNextConsequence().clientNonce);
			}
			try (ListenerConnection followerExplicit = ListenerConnection.open(leaderClientAddress, explicit, 0L)) {
				Assert.assertEquals(result4.message.nonce, followerExplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result6.message.nonce, followerExplicit.pollForNextConsequence().clientNonce);
				Assert.assertEquals(result8.message.nonce, followerExplicit.pollForNextConsequence().clientNonce);
			}
		} finally {
			// Shut down.
			Assert.assertEquals(0, leader.stop());
			Assert.assertEquals(0, follower.stop());
		}
	}

	/**
	 * Tests that downstream connections are automatically re-established when broken.
	 */
	@Test
	public void testDownstreamReconnection() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		UUID leaderUuid = UUID.randomUUID();
		ServerWrapper leader = ServerWrapper.startedServerWrapperWithUuid("testDownstreamReconnection-LEADER", leaderUuid, 3001, 2001, _folder.newFolder());
		InetSocketAddress leaderClientAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		UUID followerUuid = UUID.randomUUID();
		ServerWrapper follower = ServerWrapper.startedServerWrapperWithUuid("testDownstreamReconnection-FOLLOWER", followerUuid, 3002, 2002, _folder.newFolder());
		InetSocketAddress followerClientAddress= new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try(ClientConnection client = ClientConnection.open(leaderClientAddress)) {
			ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
					new ConfigEntry(leaderUuid, new InetSocketAddress(InetAddress.getLocalHost(), 3001), leaderClientAddress),
					new ConfigEntry(followerUuid, new InetSocketAddress(InetAddress.getLocalHost(), 3002), followerClientAddress),
			});
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendUpdateConfig(config).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			
			// Stop the follower, send another message, wait for it to be received.
			Assert.assertEquals(0, follower.stop());
			ClientResult result1 = client.sendPut(topic, new byte[0], new byte[] {1});
			result1.waitForReceived();
			
			// Now restart the follower, send another message, and wait for both to commit.
			follower = ServerWrapper.startedServerWrapperWithUuid("testDownstreamReconnection-FOLLOWER", followerUuid, 3002, 2002, _folder.newFolder());
			ClientResult result2 = client.sendPut(topic, new byte[0], new byte[] {2});
			Assert.assertEquals(CommitInfo.Effect.VALID, result1.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, result2.waitForCommitted().effect);
			
			// Now, attach a listener the follower and make sure we see the create and the 2 puts.
			try (ListenerConnection leaderImplicit = ListenerConnection.open(followerClientAddress, topic, 0L)) {
				Assert.assertEquals(Consequence.Type.TOPIC_CREATE, leaderImplicit.pollForNextConsequence().type);
				Assert.assertEquals(Consequence.Type.KEY_PUT, leaderImplicit.pollForNextConsequence().type);
				Assert.assertEquals(Consequence.Type.KEY_PUT, leaderImplicit.pollForNextConsequence().type);
			}
		} finally {
			// Shut down.
			Assert.assertEquals(0, leader.stop());
			Assert.assertEquals(0, follower.stop());
		}
	}


	private Consequence[] _listenOnServer(InetSocketAddress serverAddress, TopicName topic, UUID clientUuid, int count) throws Throwable {
		CaptureListener listener = new CaptureListener(serverAddress, topic, count);
		listener.skipNonceCheck(clientUuid, 1L);
		listener.start();
		return listener.waitForTerminate();
	}

	private void _runBatch(ClientConnection client, TopicName topic, int size, int bias) throws Throwable {
		ClientResult results[] = new ClientResult[size];
		for (int i = 0; i < results.length; ++i) {
			results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)(i + bias)});
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
		return ServerWrapper.startedServerWrapperWithUuid(name + "-" + count, uuid, 3000 + count, 2000 + count, _folder.newFolder());
	}
}
