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
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;


/**
 * Tests the DoNothing contract in a clustered context.
 */
public class TestDoNothingCluster {
	@Test
	public void testDeployment() throws Throwable {
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testDeployment-LEADER", 2001, 3001, new File("/tmp/laminar1"));
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testDeployment-FOLLOWER", 2002, 3002, new File("/tmp/laminar2"));
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		InetSocketAddress followerAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3002);
		
		// Connect the cluster.
		_runConfigBuilder(new String[] {
				leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()),
				followerAddress.getAddress().getHostAddress(), Integer.toString(followerAddress.getPort()),
		});
		
		// Create the topic and prepare the deployment.
		TopicName topic = TopicName.fromString("test");
		byte[] jar = ContractPackager.createJarForClass(DoNothing.class);
		byte[] args = new byte[0];
		
		// Deploy the contract so we can verify that the listener sees it on both nodes.
		try (ClientConnection client = ClientConnection.open(leaderAddress)) {
			// Deploy.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateProgrammableTopic(topic, jar, args).waitForCommitted().effect);
			// We know that this contract won't generate events but messages to it should still commit, successfully.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendPut(topic, new byte[32], new byte[] {1}).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendDelete(topic, new byte[32]).waitForCommitted().effect);
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
}
