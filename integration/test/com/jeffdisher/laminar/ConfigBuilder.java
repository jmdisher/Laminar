package com.jeffdisher.laminar;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.Assert;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.message.ClientMessage;


/**
 * A tool to build and post a config to a cluster.
 * Args:
 * -(ip port)+ - each entry is 2 args: client ip and client port
 * 
 * Contacts each of the listed nodes as a client, requesting their own config descriptions.
 */
public class ConfigBuilder {
	public static void main(String[] args) throws Throwable {
		// Parse arguments.
		InetSocketAddress[] peers = new InetSocketAddress[args.length / 2];
		for (int i = 0; i < args.length; i += 2) {
			peers[i / 2] = new InetSocketAddress(args[i], Integer.parseInt(args[i + 1]));
		}
		
		// Print out plan.
		System.out.println("Creating config of peers:");
		for (InetSocketAddress peer : peers) {
			System.out.println("\t" + peer);
		}
		
		// Fetch ConfigEntry from each peer.
		System.out.println("Contacting peers:");
		ConfigEntry[] entries = new ConfigEntry[peers.length];
		for (int i = 0; i < peers.length; ++i) {
			InetSocketAddress peer = peers[i];
			System.out.println("\t" + peer + "...");
			entries[i] = _getConfigEntry(peer);
			System.out.println("\t\t" + entries[i]);
		}
		
		// Create and post config.
		ClusterConfig config = ClusterConfig.configFromEntries(entries);
		System.out.println("Sending new config to leader: " + peers[0] + "...");
		try (ClientConnection client = ClientConnection.open(peers[0])) {
			ClientResult result = client.sendUpdateConfig(config);
			result.waitForReceived();
			System.out.println("\tConfig received!  Waiting for commit (ctrl-c at this point is safe - config _will_ commit)...");
			CommitInfo info = result.waitForCommitted();
			if (CommitInfo.Effect.VALID == info.effect) {
				System.out.println("\tCommitted successfully at offset " + info.mutationOffset);
			} else {
				System.out.println("\tCommit failed with effect " + info.effect + " at offset " + info.mutationOffset);
			}
		}
	}

	private static ConfigEntry _getConfigEntry(InetSocketAddress peer) throws IOException {
		SocketChannel socket = SocketChannel.open();
		socket.configureBlocking(true);
		socket.connect(peer);
		
		ClientMessage message = ClientMessage.getSelfConfig();
		byte[] serialized = message.serialize();
		_writeFramedMessage(socket, serialized);
		byte[] rawConfig = _readFramedMessage(socket);
		return ConfigEntry.deserializeFrom(ByteBuffer.wrap(rawConfig));
	}

	private static void _writeFramedMessage(SocketChannel socket, byte[] serialized) throws IOException {
		ByteBuffer toSend = ByteBuffer.allocate(Short.BYTES + serialized.length)
				.putShort((short)serialized.length)
				.put(serialized)
		;
		toSend.flip();
		while (toSend.hasRemaining()) {
			socket.write(toSend);
		}
	}

	private static byte[] _readFramedMessage(SocketChannel socket) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		int read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		buffer.flip();
		int size = Short.toUnsignedInt(buffer.getShort());
		buffer = ByteBuffer.allocate(size);
		read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		return buffer.array();
	}
}
