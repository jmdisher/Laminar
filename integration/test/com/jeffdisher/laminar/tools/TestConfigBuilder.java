package com.jeffdisher.laminar.tools;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.ProcessWrapper;
import com.jeffdisher.laminar.ServerWrapper;
import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


/**
 * Integration tests of ConfigBuilder to ensure that it can build and post a cluster config for a bunch of nodes.
 * This requires env vars to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 * -CONFIG_BUILDER_JAR - points to the JAR of the ConfigBuilder tool
 */
public class TestConfigBuilder {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	/**
	 * Starts 2 nodes, asks ConfigBuilder to build a config and post it to one of them.
	 * Then creates a normal client to post a normal message to the cluster and verifies that listeners attached to each
	 * node see the event.
	 */
	@Test
	public void test2NodeCluster() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		ServerWrapper leader = ServerWrapper.startedServerWrapper("test2NodeCluster-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("test2NodeCluster-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Invoke the builder - it will return once the config change commits.
		_runConfigBuilder(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort())});
		
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
		
		Consequence leaderRecord = leaderListener.pollForNextConsequence();
		Assert.assertEquals(Consequence.Type.KEY_PUT, leaderRecord.type);
		Assert.assertArrayEquals(messageKey, ((Payload_KeyPut) leaderRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((Payload_KeyPut) leaderRecord.payload).value);
		
		Consequence followerRecord = followerListener.pollForNextConsequence();
		Assert.assertEquals(Consequence.Type.KEY_PUT, followerRecord.type);
		Assert.assertArrayEquals(messageKey, ((Payload_KeyPut) followerRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((Payload_KeyPut) followerRecord.payload).value);
		
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
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testClusterChange-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower1 = ServerWrapper.startedServerWrapper("testClusterChange-FOLLOWER1", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress follower1Address = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Invoke the builder - it will return once the config change commits.
		_runConfigBuilder(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), follower1Address.getAddress().getHostAddress(), Integer.toString(follower1Address.getPort())});
		
		ServerWrapper follower2 = ServerWrapper.startedServerWrapper("testClusterChange-FOLLOWER2", 2003, 3003, _folder.newFolder());
		InetSocketAddress follower2Address = new InetSocketAddress(InetAddress.getLocalHost(), 3003);
		// Invoke the builder - it will return once the config change commits.
		_runConfigBuilder(new String[] {leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), follower2Address.getAddress().getHostAddress(), Integer.toString(follower2Address.getPort())});
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
		
		Consequence leaderRecord = leaderListener.pollForNextConsequence();
		Assert.assertEquals(Consequence.Type.KEY_PUT, leaderRecord.type);
		Assert.assertArrayEquals(messageKey, ((Payload_KeyPut) leaderRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((Payload_KeyPut) leaderRecord.payload).value);
		
		Consequence followerRecord = followerListener.pollForNextConsequence();
		Assert.assertEquals(Consequence.Type.KEY_PUT, followerRecord.type);
		Assert.assertArrayEquals(messageKey, ((Payload_KeyPut) followerRecord.payload).key);
		Assert.assertArrayEquals(messageValue, ((Payload_KeyPut) followerRecord.payload).value);
		
		leaderListener.close();
		followerListener.close();
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower2.stop());
	}

	@Test
	public void testInvocationError() throws Throwable {
		_runConfigBuilderWithResult(1, new String[0]);
	}


	private static void _runConfigBuilder(String[] mainArgs) throws Throwable {
		_runConfigBuilderWithResult(0, mainArgs);
	}

	private static void _runConfigBuilderWithResult(int resultCode, String[] mainArgs) throws Throwable {
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
		Assert.assertEquals(resultCode, process.waitForTermination());
	}
}
