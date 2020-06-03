package com.jeffdisher.laminar.contracts;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

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
 * Tests the EvenMutationOffsetOnly contract in a clustered context.
 */
public class TestEvenMutationOffsetOnlyCluster {
	@Test
	public void testSimpleUsage() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testSimpleUsage-LEADER", 2001, 3001, new File("/tmp/laminar1"));
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testSimpleUsage-FOLLOWER", 2002, 3002, new File("/tmp/laminar2"));
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Create the topic and prepare the deployment.
		TopicName topic = TopicName.fromString("test");
		byte[] jar = ContractPackager.createJarForClass(EvenMutationOffsetOnly.class);
		byte[] args = new byte[0];
		
		// Deploy the contract, send a PUT, a DELETE, and another PUT.
		// (keys need to be 32-bytes).
		byte[] key = new byte[32];
		key[0] = 1;
		byte[] value1 = new byte[] {1,2,3};
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
			// Mutation 3 will fail.
			Assert.assertEquals(CommitInfo.Effect.ERROR, client.sendPut(topic, key, value1).waitForCommitted().effect);
			// Mutation 4 will pass.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendDelete(topic, key).waitForCommitted().effect);
			// Mutation 5 will fail but 6 will pass.
			Assert.assertEquals(CommitInfo.Effect.ERROR, client.sendPut(topic, key, value1).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendPut(topic, key, value1).waitForCommitted().effect);
		}
		
		// Poll for events on both the leader and follower:  creation, DELETE, PUT.
		EventRecord[] leaderEvents = _pollEvents(leaderAddress, topic, 3);
		_verifyCreateEvent(leaderEvents[0], 1L, 2L, 1L, jar, args);
		_verifyDeleteEvent(leaderEvents[1], 1L, 4L, 2L, key);
		_verifyPutEvent(leaderEvents[2], 1L, 6L, 3L, key, value1);
		EventRecord[] followerEvents = _pollEvents(followerAddress, topic, 3);
		_verifyCreateEvent(followerEvents[0], 1L, 2L, 1L, jar, args);
		_verifyDeleteEvent(followerEvents[1], 1L, 4L, 2L, key);
		_verifyPutEvent(followerEvents[2], 1L, 6L, 3L, key, value1);
		
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
