package com.jeffdisher.laminar.contracts;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

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
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;


/**
 * Tests the SimpleDeployment contract in a clustered context.
 */
public class TestSimpleDeploymentCluster {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testDeployment() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testDeployment-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testDeployment-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Create the topic and prepare the deployment.
		TopicName topic = TopicName.fromString("test");
		byte[] jar = ContractPackager.createJarForClass(SimpleDeployment.class);
		byte[] args = new byte[32];
		args[0] = 5;
		
		// Deploy the contract so we can verify that the listener sees it on both nodes.
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			// Deploy.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
		}
		
		// Poll for events on both the leader and follower and see the programmable creation and the other event it writes.
		Consequence[] leaderEvent = _pollEvents(leaderAddress, topic, 2);
		_verifyCreateEvent(leaderEvent[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(leaderEvent[1], 1L, 2L, 2L, args, new byte[] {0});
		Consequence[] followerEvent = _pollEvents(followerAddress, topic, 2);
		_verifyCreateEvent(followerEvent[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(followerEvent[1], 1L, 2L, 2L, args, new byte[] {0});
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}

	@Test
	public void testSimpleMessages() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testSimpleMessages-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testSimpleMessages-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Create the topic and prepare the deployment.
		TopicName topic = TopicName.fromString("test");
		byte[] jar = ContractPackager.createJarForClass(SimpleDeployment.class);
		byte[] args = new byte[32];
		args[0] = 5;
		
		// Deploy the contract so we can verify that the listener sees it on both nodes.
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			// Deploy.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
			// Send PUT.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendPut(topic, args, new byte[0]).waitForCommitted().effect);
			// Send DELETE.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendDelete(topic, args).waitForCommitted().effect);
		}
		
		// Poll for events on both the leader and follower and see the programmable creation and the other event it writes.
		Consequence[] leaderEvent = _pollEvents(leaderAddress, topic, 4);
		_verifyCreateEvent(leaderEvent[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(leaderEvent[1], 1L, 2L, 2L, args, new byte[] {0});
		_verifyPutEvent(leaderEvent[2], 1L, 3L, 3L, args, new byte[] {1});
		_verifyPutEvent(leaderEvent[3], 1L, 4L, 4L, args, new byte[] {2});
		Consequence[] followerEvent = _pollEvents(followerAddress, topic, 4);
		_verifyCreateEvent(followerEvent[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(followerEvent[1], 1L, 2L, 2L, args, new byte[] {0});
		_verifyPutEvent(followerEvent[2], 1L, 3L, 3L, args, new byte[] {1});
		_verifyPutEvent(followerEvent[3], 1L, 4L, 4L, args, new byte[] {2});
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}


	private static Consequence[] _pollEvents(InetSocketAddress serverAddress, TopicName topicName, int count) throws Throwable {
		Consequence[] events = new Consequence[count];
		try (ListenerConnection listener = ListenerConnection.open(serverAddress, topicName, 0L)) {
			for (int i = 0; i < count; ++i) {
				events[i] = listener.pollForNextConsequence();
			}
		}
		return events;
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

	private void _verifyCreateEvent(Consequence eventRecord, long termNumber, long mutationOffset, long eventOffset, byte[] code, byte[] arguments) {
		Assert.assertEquals(Consequence.Type.TOPIC_CREATE, eventRecord.type);
		Assert.assertEquals(termNumber, eventRecord.termNumber);
		Assert.assertEquals(mutationOffset, eventRecord.globalOffset);
		Assert.assertEquals(eventOffset, eventRecord.consequenceOffset);
		Assert.assertArrayEquals(code, ((Payload_TopicCreate)eventRecord.payload).code);
		Assert.assertArrayEquals(arguments, ((Payload_TopicCreate)eventRecord.payload).arguments);
	}

	private void _verifyPutEvent(Consequence eventRecord, long termNumber, long mutationOffset, long eventOffset, byte[] key, byte[] value) {
		Assert.assertEquals(Consequence.Type.KEY_PUT, eventRecord.type);
		Assert.assertEquals(termNumber, eventRecord.termNumber);
		Assert.assertEquals(mutationOffset, eventRecord.globalOffset);
		Assert.assertEquals(eventOffset, eventRecord.consequenceOffset);
		Assert.assertArrayEquals(key, ((Payload_KeyPut)eventRecord.payload).key);
		Assert.assertArrayEquals(value, ((Payload_KeyPut)eventRecord.payload).value);
	}
}
