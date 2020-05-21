package com.jeffdisher.laminar;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.event.EventRecordPayload_Put;
import com.jeffdisher.laminar.types.event.EventRecordType;


/**
 * Integration tests of ConfigBuilder to ensure that it can build and post a cluster config for a bunch of nodes.
 */
public class TestConfigBuilder {
	/**
	 * Starts 2 nodes, asks ConfigBuilder to build a config and post it to one of them.
	 * Then creates a normal client to post a normal message to the cluster and verifies that listeners attached to each
	 * node see the event.
	 */
	@Test
	public void test2NodeCluster() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("test2NodeCluster-LEADER", 2001, 3001, new File("/tmp/laminar1"));
		ServerWrapper follower = ServerWrapper.startedServerWrapper("test2NodeCluster-FOLLOWER", 2002, 3002, new File("/tmp/laminar2"));
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Invoke the builder - it will return once the config change commits.
		// (HACK: we need to wait for the servers to start up).
		Thread.sleep(500);
		ConfigBuilder.main(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort())});
		
		// Create the client and send the normal update (use the follower address to force the redirect).
		byte[] messageKey = new byte[] {1,2,3};
		byte[] messageValue = new byte[] {4,5,6,7,8};
		try (ClientConnection client = ClientConnection.open(followerAddress)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendPut(topic, messageKey, messageValue);
			CommitInfo info = result.waitForCommitted();
			// We expect offset three since UPDATE_CONFIG and CREATE_TOPIC came first.
			Assert.assertEquals(3L, info.mutationOffset);
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
		}
		
		// Create the listener on both the leader and follower to make sure they both see this message (start at local offset 1L to skip the CREATE_TOPIC).
		ListenerConnection leaderListener = ListenerConnection.open(leaderAddress, topic, 1L);
		ListenerConnection followerListener = ListenerConnection.open(followerAddress, topic, 1L);
		
		EventRecord leaderRecord = leaderListener.pollForNextEvent();
		Assert.assertEquals(EventRecordType.PUT, leaderRecord.type);
		Assert.assertArrayEquals(messageKey, ((EventRecordPayload_Put) leaderRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((EventRecordPayload_Put) leaderRecord.payload).value);
		
		EventRecord followerRecord = followerListener.pollForNextEvent();
		Assert.assertEquals(EventRecordType.PUT, followerRecord.type);
		Assert.assertArrayEquals(messageKey, ((EventRecordPayload_Put) followerRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((EventRecordPayload_Put) followerRecord.payload).value);
		
		leaderListener.close();
		followerListener.close();
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}

	/**
	 * Starts 2 nodes, asks ConfigBuilder to build a config and post it to one of them.
	 * Then starts another node, asks the builder to create a new config which excludes the earlier follower.
	 * Once this commits, stops the earlier follower.
	 * Finally creates a normal client to post a normal message to the cluster and verifies that listeners attached to
	 * each of the final 2 nodes sees the event.
	 */
	@Test
	public void testClusterChange() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testClusterChange-LEADER", 2001, 3001, new File("/tmp/laminar1"));
		ServerWrapper follower1 = ServerWrapper.startedServerWrapper("testClusterChange-FOLLOWER1", 2002, 3002, new File("/tmp/laminar2"));
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress follower1Address = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Invoke the builder - it will return once the config change commits.
		// (HACK: we need to wait for the servers to start up).
		Thread.sleep(500);
		ConfigBuilder.main(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), follower1Address.getAddress().getHostAddress(), Integer.toString(follower1Address.getPort())});
		
		ServerWrapper follower2 = ServerWrapper.startedServerWrapper("testClusterChange-FOLLOWER2", 2003, 3003, new File("/tmp/laminar3"));
		InetSocketAddress follower2Address = new InetSocketAddress(InetAddress.getLocalHost(), 3003);
		// Invoke the builder - it will return once the config change commits.
		// (HACK: we need to wait for the servers to start up).
		Thread.sleep(500);
		ConfigBuilder.main(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), follower2Address.getAddress().getHostAddress(), Integer.toString(follower2Address.getPort())});
		Assert.assertEquals(0, follower1.stop());
		
		// Create the client and send the normal update (use the follower address to force the redirect).
		byte[] messageKey = new byte[] {1,2,3};
		byte[] messageValue = new byte[] {4,5,6,7,8};
		try (ClientConnection client = ClientConnection.open(follower2Address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendPut(topic, messageKey, messageValue);
			CommitInfo info = result.waitForCommitted();
			// We expect offset three since UPDATE_CONFIG, UPDATE_CONFIG, and CREATE_TOPIC came first.
			Assert.assertEquals(4L, info.mutationOffset);
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
		}
		
		// Create the listener on both the leader and follower to make sure they both see this message (start at local offset 1L to skip the CREATE_TOPIC).
		ListenerConnection leaderListener = ListenerConnection.open(leaderAddress, topic, 1L);
		ListenerConnection followerListener = ListenerConnection.open(follower2Address, topic, 1L);
		
		EventRecord leaderRecord = leaderListener.pollForNextEvent();
		Assert.assertEquals(EventRecordType.PUT, leaderRecord.type);
		Assert.assertArrayEquals(messageKey, ((EventRecordPayload_Put) leaderRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((EventRecordPayload_Put) leaderRecord.payload).value);
		
		EventRecord followerRecord = followerListener.pollForNextEvent();
		Assert.assertEquals(EventRecordType.PUT, followerRecord.type);
		Assert.assertArrayEquals(messageKey, ((EventRecordPayload_Put) followerRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((EventRecordPayload_Put) followerRecord.payload).value);
		
		leaderListener.close();
		followerListener.close();
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower2.stop());
	}
}
