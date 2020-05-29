package com.jeffdisher.laminar.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.event.EventRecordType;
import com.jeffdisher.laminar.types.payload.Payload_Put;
import com.jeffdisher.laminar.utils.Assert;


/**
 * An interactive client example to demonstrate basics of Laminar and make its actions observable.
 * Starts a client and listener, internally, using a topic name as a chat room.
 * Lines submitted on STDIN will be posted to the topic and anything the listener observes will be written to STDOUT
 * with the UUID of the client who posted it.
 * There is no active effort to avoid these 2 operations "fighting over the console" as this is just a basic example,
 * not a real application.
 * 
 * Args (just as a list):
 * -ip - the IP of one of the nodes in the cluster
 * -port - the client port of that node
 * -topic - the chat room name
 * -name - The name the user should use to identity themselves in the room.
 */
public class ChatServerExample {
	public static void main(String[] args) throws Throwable {
		if (4 != args.length) {
			_usageAndExit();
		}
		InetAddress ip = InetAddress.getByName(args[0]);
		int port = Integer.parseInt(args[1]);
		InetSocketAddress address = new InetSocketAddress(ip, port);
		TopicName topicName = TopicName.fromString(args[2]);
		String userName = args[3];
		System.out.print(userName + " connecting to: " + address + " on topic: " + topicName + "...");
		
		ClientConnection client = ClientConnection.open(address);
		ListenerConnection listener = ListenerConnection.open(address, topicName, 0L);
		client.waitForConnectionOrFailure();
		listener.waitForConnectionOrFailure();
		System.out.println(" Connected!");
		System.out.println("Type \"stop\" to exit.");
		Thread listenerThread = new Thread(() -> {
			try {
				EventRecord record = listener.pollForNextEvent();
				while (null != record) {
					// If this is a put, write it to STDOUT.
					if (EventRecordType.PUT == record.type) {
						String name = new String(((Payload_Put) record.payload).key, StandardCharsets.UTF_8);
						String text = new String(((Payload_Put) record.payload).value, StandardCharsets.UTF_8);
						System.out.println(name + ": " + text);
					}
					record = listener.pollForNextEvent();
				}
			} catch (InterruptedException e) {
				// We don't use interruption.
				throw Assert.unexpected(e);
			}
		});
		listenerThread.start();
		
		// Start by making sure that there is a topic for the room.
		CommitInfo createInfo = client.sendCreateTopic(topicName).waitForCommitted();
		if (CommitInfo.Effect.VALID == createInfo.effect) {
			System.out.println("(room created)");
		}
		
		byte[] rawUser = userName.getBytes(StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		boolean keepRunning = true;
		while (keepRunning) {
			String line = reader.readLine();
			if ("stop".equals(line)) {
				keepRunning = false;
			} else {
				byte[] rawLine = line.getBytes(StandardCharsets.UTF_8);
				CommitInfo info = client.sendPut(topicName, rawUser, rawLine).waitForCommitted();
				if (CommitInfo.Effect.VALID != info.effect) {
					System.err.println("INVALID COMMIT!");
				}
			}
		}
		
		client.close();
		listener.close();
		listenerThread.join();
	}


	private static void _usageAndExit() {
		System.err.println("Usage: ChatServerExample <ip> <port> <chatroom> <username>");
		System.exit(1);
	}
}
