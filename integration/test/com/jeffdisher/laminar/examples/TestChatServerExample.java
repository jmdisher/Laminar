package com.jeffdisher.laminar.examples;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.ProcessWrapper;
import com.jeffdisher.laminar.ServerWrapper;


/**
 * Integration test of the ChatServerExample.
 * This requires env vars to be set:
 * -WRAPPER_SERVER_JAR - points to the JAR of the complete Laminar server
 * -CHAT_CLIENT_JAR - points to the JAR of the ChatServerExample
 */
public class TestChatServerExample {
	/**
	 * Starts 1 node and attaches 2 clients to it, verifying that the first sees the topic created and that both see
	 * each other speaking.
	 */
	@Test
	public void test2Clients() throws Throwable {
		String roomName = "chatroom";
		ServerWrapper leader = ServerWrapper.startedServerWrapper("test2Clients-LEADER", 2001, 3001, new File("/tmp/laminar1"));
		InetSocketAddress leaderAddress = new InetSocketAddress(InetAddress.getLocalHost(), 3001);
		
		// (HACK: we need to wait for the servers to start up).
		Thread.sleep(500);
		ProcessWrapper user1 = _startChatClient("user1", leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), roomName, "user1");
		CountDownLatch roomCreation = user1.filterStdout("(room created)");
		user1.startFiltering();
		roomCreation.await();
		ProcessWrapper user2 = _startChatClient("user2", leaderAddress.getAddress().getHostAddress(), Integer.toString(leaderAddress.getPort()), roomName, "user2");
		user2.startFiltering();
		
		// Both clients should see the other message and their own.
		CountDownLatch fromUser1 = user2.filterStdout("user1: Hello");
		CountDownLatch echoUser1 = user1.filterStdout("user1: Hello");
		CountDownLatch fromUser2 = user1.filterStdout("user2: I see you");
		CountDownLatch echoUser2 = user2.filterStdout("user2: I see you");
		
		// Send both messages.
		user1.sendStdin("Hello");
		user2.sendStdin("I see you");
		
		// Wait for everything.
		fromUser1.await();
		echoUser1.await();
		fromUser2.await();
		echoUser2.await();
		
		// Shut down.
		user1.sendStdin("stop");
		user2.sendStdin("stop");
		Assert.assertEquals(0, user1.waitForTermination());
		Assert.assertEquals(0, user2.waitForTermination());
		Assert.assertEquals(0, leader.stop());
	}


	private static ProcessWrapper _startChatClient(String clientName, String... mainArgs) throws Throwable {
		String jarPath = System.getenv("CHAT_CLIENT_JAR");
		if (null == jarPath) {
			throw new IllegalArgumentException("Missing CHAT_CLIENT_JAR env var");
		}
		if (!new File(jarPath).exists()) {
			throw new IllegalArgumentException("JAR \"" + jarPath + "\" doesn't exist");
		}
		
		return ProcessWrapper.startedJavaProcess(clientName, jarPath, mainArgs);
	}
}
