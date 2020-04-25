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
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;


public class TestClientManager {
	private static final int PORT_BASE = 3100;

	/**
	 * In this test, we start a ClientManager and emulate a client connecting to it and sending a temp message.
	 * Note that there isn't any other logic in the ClientManager, itself, so we just make sure it sees the correct
	 * message.
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
			// Write our handshake to end up in the "normal client" state.
			_writeFramedMessage(toServer, ClientMessage.handshake(UUID.randomUUID()).serialize());
			// Run the callbacks once to allow the ClientManager to do the state transition.
			ClientMessage readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			// (we need to run it a second time because of the way the CLIENT_READY is queued)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
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
	}

	@Test
	public void testSendCommitResponse() throws Throwable {
		// Create a commit response.
		ClientResponse commit = ClientResponse.committed(1L, 1L);
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
			UUID clientId = UUID.randomUUID();
			_writeFramedMessage(client.getOutputStream(), ClientMessage.handshake(clientId).serialize());
			
			InputStream fromServer = client.getInputStream();
			manager.testingSend(connectedNode, commit);
			// Allocate the frame for the full buffer we know we are going to read.
			byte[] serialized = commit.serialize();
			byte[] raw = _readFramedMessage(fromServer);
			Assert.assertEquals(serialized.length, raw.length);
			// Deserialize the buffer.
			ClientResponse deserialized = ClientResponse.deserialize(raw);
			Assert.assertEquals(commit.type, deserialized.type);
			Assert.assertEquals(commit.nonce, deserialized.nonce);
			Assert.assertEquals(commit.lastCommitGlobalOffset, deserialized.lastCommitGlobalOffset);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
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
		private final ClusterConfig _dummyConfig = ClusterConfig.configFromEntries(new ClusterConfig.ConfigEntry[] {new ClusterConfig.ConfigEntry(new InetSocketAddress(5), new InetSocketAddress(6))});
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
			_pendingConsumer.accept(new StateSnapshot(_dummyConfig, 0L, 0L));
			_pendingConsumer = null;
			this.notifyAll();
		}
	}
}
