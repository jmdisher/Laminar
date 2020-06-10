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
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.event.EventRecordType;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;


/**
 * Tests the EmulateStutter contract in a clustered context.
 */
public class TestEmulateStutterCluster {
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
		byte[] jar = ContractPackager.createJarForClass(EmulateStutter.class);
		byte[] args = new byte[0];
		
		// Deploy the contract so we can verify that the listener sees it on both nodes.
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
		}
		
		// Poll for events on both the leader and follower and see the programmable creation.
		EventRecord[] leaderEvent = _pollEvents(leaderAddress, topic, 1);
		_verifyCreateEvent(leaderEvent[0], 1L, 2L, 1L, jar, args);
		EventRecord[] followerEvent = _pollEvents(followerAddress, topic, 1);
		_verifyCreateEvent(followerEvent[0], 1L, 2L, 1L, jar, args);
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}

	@Test
	public void testSimpleUsage() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testSimpleUsage-LEADER", 2001, 3001, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testSimpleUsage-FOLLOWER", 2002, 3002, _folder.newFolder());
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Create the topic and prepare the deployment.
		TopicName topic = TopicName.fromString("test");
		byte[] jar = ContractPackager.createJarForClass(EmulateStutter.class);
		byte[] args = new byte[0];
		
		// Deploy the contract, send a PUT, a DELETE, and another PUT.
		// (keys need to be 32-bytes).
		byte[] key = new byte[32];
		key[0] = 1;
		byte[] value1 = new byte[] {1,2,3};
		byte[] value2 = new byte[] {4,5};
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendPut(topic, key, value1).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendDelete(topic, key).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendPut(topic, key, value2).waitForCommitted().effect);
		}
		
		// Poll for events on both the leader and follower:  creation, 2xPUT, DELETE, 2xPUT.
		EventRecord[] leaderEvents = _pollEvents(leaderAddress, topic, 6);
		_verifyCreateEvent(leaderEvents[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(leaderEvents[1], 1L, 3L, 2L, key, value1);
		_verifyPutEvent(leaderEvents[2], 1L, 3L, 3L, key, value1);
		_verifyDeleteEvent(leaderEvents[3], 1L, 4L, 4L, key);
		_verifyPutEvent(leaderEvents[4], 1L, 5L, 5L, key, value2);
		_verifyPutEvent(leaderEvents[5], 1L, 5L, 6L, key, value2);
		EventRecord[] followerEvents = _pollEvents(followerAddress, topic, 6);
		_verifyCreateEvent(followerEvents[0], 1L, 2L, 1L, jar, args);
		_verifyPutEvent(followerEvents[1], 1L, 3L, 2L, key, value1);
		_verifyPutEvent(followerEvents[2], 1L, 3L, 3L, key, value1);
		_verifyDeleteEvent(followerEvents[3], 1L, 4L, 4L, key);
		_verifyPutEvent(followerEvents[4], 1L, 5L, 5L, key, value2);
		_verifyPutEvent(followerEvents[5], 1L, 5L, 6L, key, value2);
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}


	private static EventRecord[] _pollEvents(InetSocketAddress serverAddress, TopicName topicName, int count) throws Throwable {
		EventRecord[] events = new EventRecord[count];
		try (ListenerConnection listener = ListenerConnection.open(serverAddress, topicName, 0L)) {
			for (int i = 0; i < count; ++i) {
				events[i] = listener.pollForNextEvent();
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

	private void _verifyCreateEvent(EventRecord eventRecord, long termNumber, long mutationOffset, long eventOffset, byte[] code, byte[] arguments) {
		Assert.assertEquals(EventRecordType.TOPIC_CREATE, eventRecord.type);
		Assert.assertEquals(termNumber, eventRecord.termNumber);
		Assert.assertEquals(mutationOffset, eventRecord.globalOffset);
		Assert.assertEquals(eventOffset, eventRecord.localOffset);
		Assert.assertArrayEquals(code, ((Payload_TopicCreate)eventRecord.payload).code);
		Assert.assertArrayEquals(arguments, ((Payload_TopicCreate)eventRecord.payload).arguments);
	}

	private void _verifyPutEvent(EventRecord eventRecord, long termNumber, long mutationOffset, long eventOffset, byte[] key, byte[] value) {
		Assert.assertEquals(EventRecordType.KEY_PUT, eventRecord.type);
		Assert.assertEquals(termNumber, eventRecord.termNumber);
		Assert.assertEquals(mutationOffset, eventRecord.globalOffset);
		Assert.assertEquals(eventOffset, eventRecord.localOffset);
		Assert.assertArrayEquals(key, ((Payload_KeyPut)eventRecord.payload).key);
		Assert.assertArrayEquals(value, ((Payload_KeyPut)eventRecord.payload).value);
	}

	private void _verifyDeleteEvent(EventRecord eventRecord, long termNumber, long mutationOffset, long eventOffset, byte[] key) {
		Assert.assertEquals(EventRecordType.KEY_DELETE, eventRecord.type);
		Assert.assertEquals(termNumber, eventRecord.termNumber);
		Assert.assertEquals(mutationOffset, eventRecord.globalOffset);
		Assert.assertEquals(eventOffset, eventRecord.localOffset);
		Assert.assertArrayEquals(key, ((Payload_KeyDelete)eventRecord.payload).key);
	}
}
