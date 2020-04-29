package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClientResponseType;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;


public class TestClientManager {
	private static final int PORT_BASE = 3100;

	/**
	 * In this test, we start a ClientManager and emulate a client connecting to it and sending a temp message.
	 * This involves managing the normal HANDSHAKE and CLIENT_READY message flow prior to sending the message.
	 * Finally, we just verify that the ClientManager did observe the expected message.
	 */
	@Test
	public void testReceiveTempMessage() throws Throwable {
		// Create a message.
		ClientMessage message = ClientMessage.temp(1L, new byte[] {0,1,2,3});
		// Create a server.
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection and send the "temp" message through, directly.
		NetworkManager.NodeToken connectedNode = null;
		try (Socket client = new Socket("localhost", port)) {
			connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			OutputStream toServer = client.getOutputStream();
			InputStream fromServer = client.getInputStream();
			
			// Write our handshake to end up in the "normal client" state.
			_writeFramedMessage(toServer, ClientMessage.handshake(UUID.randomUUID()).serialize());
			
			// Run the callbacks once to allow the ClientManager to do the state transition.
			ClientMessage readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			// (we need to run it a second time because of the way the CLIENT_READY is queued)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Read the CLIENT_READY.
			byte[] raw = _readFramedMessage(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			Assert.assertEquals(0L, ready.lastCommitGlobalOffset);
			Assert.assertEquals(1L, ready.nonce);
			Assert.assertNotNull(ClusterConfig.deserialize(ready.extraData));
			
			// Write the message.
			_writeFramedMessage(toServer, message.serialize());
		}
		// We should see the message appear in callbacks.
		ClientMessage output = callbacks.runAndGetNextMessage();
		Assert.assertNotNull(output);
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		Assert.assertEquals(message.type, output.type);
		Assert.assertEquals(message.nonce, output.nonce);
		Assert.assertArrayEquals(((ClientMessagePayload_Temp)message.payload).contents, ((ClientMessagePayload_Temp)output.payload).contents);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Does the usual handshake but then sends a message and verifies that the client sees the RECEIVED and, once the
	 * manager is told the commit happened, the COMMITTED.
	 */
	@Test
	public void testSendCommitResponse() throws Throwable {
		// Create a message.
		ClientMessage message = ClientMessage.temp(1L, new byte[] {0,1,2,3});
		// Create a server.
		int port = PORT_BASE + 2;
		ServerSocketChannel socket = createSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			NetworkManager.NodeToken connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			OutputStream toServer = client.getOutputStream();
			InputStream fromServer = client.getInputStream();
			
			// Write our handshake to end up in the "normal client" state.
			_writeFramedMessage(toServer, ClientMessage.handshake(UUID.randomUUID()).serialize());
			
			// Run the callbacks once to allow the ClientManager to do the state transition.
			ClientMessage readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			// (we need to run it a second time because of the way the CLIENT_READY is queued)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Read the CLIENT_READY.
			byte[] raw = _readFramedMessage(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			Assert.assertEquals(0L, ready.lastCommitGlobalOffset);
			Assert.assertEquals(1L, ready.nonce);
			Assert.assertNotNull(ClusterConfig.deserialize(ready.extraData));
			
			// Write the message and read the RECEIVED.
			_writeFramedMessage(toServer, message.serialize());
			ClientMessage output = callbacks.runAndGetNextMessage();
			Assert.assertNotNull(output);
			raw = _readFramedMessage(fromServer);
			ClientResponse received = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.RECEIVED, received.type);
			Assert.assertEquals(0L, received.lastCommitGlobalOffset);
			Assert.assertEquals(1L, received.nonce);
			// (process writable)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Tell the manager we committed it and verify that we see the commit.
			manager.mainProcessingPendingMessageCommits(1L);
			raw = _readFramedMessage(fromServer);
			ClientResponse committed = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.COMMITTED, committed.type);
			Assert.assertEquals(1L, committed.lastCommitGlobalOffset);
			Assert.assertEquals(1L, committed.nonce);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	@Test
	public void testSendEvent() throws Throwable {
		// Create an event record.
		EventRecord record = EventRecord.generateRecord(EventRecordType.TEMP, 1L, 1L, UUID.randomUUID(), 1L, new byte[] { 1, 2, 3});
		// Create a server.
		int port = PORT_BASE + 3;
		ServerSocketChannel socket = createSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			NetworkManager.NodeToken connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			// Write the listen since we want to go into the listener state.
			_writeFramedMessage(client.getOutputStream(), ClientMessage.listen(0L).serialize());
			InputStream fromServer = client.getInputStream();
			
			// Run 1 callback to receive the LISTEN.
			callbacks.runRunnableAndGetNewClientNode(manager);
			// Consume the config it sent in response.
			_readFramedMessage(fromServer);
			// Run the next callback so the listener becomes writable.
			callbacks.runRunnableAndGetNewClientNode(manager);
			
			manager.mainSendRecordToListeners(record);
			// Allocate the frame for the full buffer we know we are going to read.
			byte[] serialized = record.serialize();
			byte[] raw = _readFramedMessage(fromServer);
			Assert.assertEquals(serialized.length, raw.length);
			// Deserialize the buffer.
			EventRecord deserialized = EventRecord.deserialize(raw);
			Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
			Assert.assertEquals(record.localOffset, deserialized.localOffset);
			Assert.assertEquals(record.clientId, deserialized.clientId);
			Assert.assertArrayEquals(record.payload, deserialized.payload);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Tests that we get a redirect message, on the client side, when telling the ClientManager to go into a FOLLOWER
	 * state.
	 */
	@Test
	public void testClientRedirectExisting() throws Throwable {
		// Create a server.
		int port = PORT_BASE + 4;
		ServerSocketChannel socket = createSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			// Send the HANDSHAKE and wait for the CLIENT_READY.
			UUID clientId = UUID.randomUUID();
			// -nodeDidConnect
			callbacks.runAndGetNextMessage();
			_writeFramedMessage(client.getOutputStream(), ClientMessage.handshake(clientId).serialize());
			// -nodeReadReady
			callbacks.runAndGetNextMessage();
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			InputStream fromServer = client.getInputStream();
			byte[] raw = _readFramedMessage(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			
			// Now, tell the ClientManager to enter the follower state.
			ConfigEntry entry = new ConfigEntry(new InetSocketAddress(9999), new InetSocketAddress(port+1));
			manager.mainEnterFollowerState(entry, 0L);
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			raw = _readFramedMessage(fromServer);
			ClientResponse redirect = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.REDIRECT, redirect.type);
			Assert.assertEquals(-1L, redirect.nonce);
			Assert.assertEquals(0L, redirect.lastCommitGlobalOffset);
			Assert.assertEquals(entry, ConfigEntry.deserializeFrom(ByteBuffer.wrap(redirect.extraData)));
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Tests that we get a new client gets a redirect immediately after handshake when in FOLLOWER state.
	 */
	@Test
	public void testClientRedirectNew() throws Throwable {
		// Create a server.
		int port = PORT_BASE + 5;
		ServerSocketChannel socket = createSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Now, tell the ClientManager to enter the follower state.
		ConfigEntry entry = new ConfigEntry(new InetSocketAddress(9999), new InetSocketAddress(port+1));
		manager.mainEnterFollowerState(entry, 0L);
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			// Send the HANDSHAKE and wait for the REDIRECT.
			UUID clientId = UUID.randomUUID();
			// -nodeDidConnect
			callbacks.runAndGetNextMessage();
			_writeFramedMessage(client.getOutputStream(), ClientMessage.handshake(clientId).serialize());
			// -nodeReadReady
			callbacks.runAndGetNextMessage();
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			InputStream fromServer = client.getInputStream();
			byte[] raw = _readFramedMessage(fromServer);
			ClientResponse redirect = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.REDIRECT, redirect.type);
			Assert.assertEquals(-1L, redirect.nonce);
			Assert.assertEquals(0L, redirect.lastCommitGlobalOffset);
			Assert.assertEquals(entry, ConfigEntry.deserializeFrom(ByteBuffer.wrap(redirect.extraData)));
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}

	private byte[] _readFramedMessage(InputStream source) throws IOException {
		byte[] frameSize = new byte[Short.BYTES];
		int read = 0;
		while (read < frameSize.length) {
			read += source.read(frameSize, read, frameSize.length - read);
		}
		int sizeToRead = Short.toUnsignedInt(ByteBuffer.wrap(frameSize).getShort());
		byte[] frame = new byte[sizeToRead];
		read = 0;
		while (read < frame.length) {
			read += source.read(frame, read, frame.length - read);
		}
		return frame;
	}

	private void _writeFramedMessage(OutputStream sink, byte[] raw) throws IOException {
		byte[] frame = new byte[Short.BYTES + raw.length];
		ByteBuffer.wrap(frame)
			.putShort((short)raw.length)
			.put(raw)
			;
		sink.write(frame);
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IClientManagerCallbacks {
		private final ClusterConfig _dummyConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(new InetSocketAddress(5), new InetSocketAddress(6))});
		private Consumer<StateSnapshot> _pendingConsumer;
		public ClientMessage recentMessage;
		
		public synchronized NetworkManager.NodeToken runRunnableAndGetNewClientNode(ClientManager managerToRead) throws InterruptedException {
			_lockedRunOnce();
			NetworkManager.NodeToken node = managerToRead.testingGetOneClientNode();
			return node;
		}
		
		public synchronized ClientMessage runAndGetNextMessage() throws InterruptedException {
			_lockedRunOnce();
			ClientMessage message = this.recentMessage;
			this.recentMessage = null;
			return message;
		}

		@Override
		public synchronized void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command) {
			while (null != _pendingConsumer) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption in this test - this is just for lock-step connection testing.
					Assert.fail(e.getLocalizedMessage());
				}
			}
			_pendingConsumer = command;
			this.notifyAll();
		}

		@Override
		public long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming) {
			this.recentMessage = incoming;
			return 1L;
		}

		@Override
		public void mainRequestMutationFetch(long mutationOffsetToFetch) {
			Assert.fail("Not used in test");
		}

		@Override
		public void mainRequestEventFetch(long nextLocalEventToFetch) {
			Assert.fail("Not used in test");
		}

		private void _lockedRunOnce() throws InterruptedException {
			while (null == _pendingConsumer) {
				this.wait();
			}
			_pendingConsumer.accept(new StateSnapshot(_dummyConfig, 0L, 0L, 0L));
			_pendingConsumer = null;
			this.notifyAll();
		}
	}
}
