package com.jeffdisher.laminar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientResponse;
import com.jeffdisher.laminar.network.ClientResponseType;
import com.jeffdisher.laminar.types.EventRecord;


/**
 * It doesn't make sense to test the entire system from within a unit test but this gives a quick way of verifying the
 * top-level system is correct before fleshing out the actual function units.
 * This will be replaced with some kind of integration test, later on.
 */
public class TestLaminar {
	private InputStream realIn;
	private PrintStream realOut;
	private PrintStream realErr;

	private ByteArrayOutputStream out;
	private ByteArrayOutputStream err;

	@Before
	public void beforeEach() {
		this.realIn = System.in;
		this.realOut= System.out;
		this.realErr = System.err;
		System.setIn(new ByteArrayInputStream(new byte[0]));
		this.out = new ByteArrayOutputStream(1024);
		System.setOut(new PrintStream(this.out));
		this.err = new ByteArrayOutputStream(1024);
		System.setErr(new PrintStream(this.err));
	}

	@After
	public void afterEach() {
		System.setIn(this.realIn);
		System.setOut(this.realOut);
		System.setErr(this.realErr);
	}

	@Test
	public void testMissingArgs() {
		boolean didFail = false;
		try {
			Laminar.main(new String[] {"missing"});
		} catch (RuntimeException e) {
			didFail = true;
		}
		Assert.assertTrue(didFail);
		byte[] errBytes = this.err.toByteArray();
		String errorString = new String(errBytes);
		Assert.assertEquals("Fatal start-up error: Missing options!  Usage:  Laminar --client <client_port> --cluster <cluster_port> --data <data_directory_path>\n"
				, errorString);
	}

	@Test
	public void testNormalRun() {
		// We just want this to start everything and then shut down.
		System.setIn(new ByteArrayInputStream("stop\n".getBytes()));
		Laminar.main(new String[] {"--client", "2000", "--cluster", "2001", "--data", "/tmp/laminar"});
	}

	@Test
	public void testSimpleClient() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress("localhost", 2002))) {
			ClientResult result = client.sendTemp("Hello World!".getBytes());
			result.waitForReceived();
			result.waitForCommitted();
		}
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testSimpleClientAndListeners() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		
		// Start a listener before the client begins.
		CountDownLatch beforeLatch = new CountDownLatch(1);
		ListenerThread beforeListener = new ListenerThread(address, message, beforeLatch);
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result = client.sendTemp(message);
			result.waitForReceived();
			result.waitForCommitted();
		}
		
		// Start a listener after the client begins.
		CountDownLatch afterLatch = new CountDownLatch(1);
		ListenerThread afterListener = new ListenerThread(address, message, afterLatch);
		afterListener.start();
		
		// Verify that both listeners received the event.
		beforeLatch.await();
		afterLatch.await();
		
		// Shut down.
		beforeListener.join();
		afterListener.join();
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testClientForceDisconnect() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result1 = client.sendTemp(message);
			ClientResult result2 = client.sendTemp(message);
			ClientResult result3 = client.sendTemp(message);
			result1.waitForReceived();
			result2.waitForReceived();
			result3.waitForReceived();
			// By this point, we know the server has received the messages so close and see how they handle this.
		}
		
		// Shut down.
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testClientFailedConnection() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Even though we aren't yet connected, the client can technically still attempt to send messages.
			client.sendTemp(message);
			// HACK:  Wait for the connection to fail.
			Thread.sleep(500);
			boolean didThrow = false;
			try {
				client.checkConnection();
			} catch (IOException e) {
				didThrow = true;
			}
			Assert.assertTrue(didThrow);
		}
	}

	@Test
	public void testListenerFailedConnection() throws Throwable {
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		CountDownLatch latch = new CountDownLatch(1);
		ListenerThread listener = new ListenerThread(address, null, latch);
		listener.start();
		// HACK:  Wait for the connection to fail.
		Thread.sleep(500);
		boolean didThrow = false;
		try {
			listener.listener.checkConnection();
		} catch (IOException e) {
			didThrow = true;
		}
		Assert.assertTrue(didThrow);
		// Shut it down and observe the we did see the null returned.
		listener.listener.close();
		latch.await();
		listener.join();
	}

	@Test
	public void testSimpleClientWaitForConnection() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		
		// It should always be harmless to wait for connection over and over so just do that here.
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress("localhost", 2002))) {
			client.waitForConnection();
			Assert.assertTrue(client.checkConnection());
			ClientResult result = client.sendTemp("Hello World!".getBytes());
			client.waitForConnection();
			Assert.assertTrue(client.checkConnection());
			result.waitForReceived();
			client.waitForConnection();
			Assert.assertTrue(client.checkConnection());
			result.waitForCommitted();
			client.waitForConnection();
			Assert.assertTrue(client.checkConnection());
		}
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testGlobalMutationCommitOffset() throws Throwable {
		// Start up a fake client to verify that the RECEIVED and COMMITTED responses have the expected commit offsets.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		
		// HACK - wait for startup.
		Thread.sleep(500);
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		UUID clientId = UUID.randomUUID();
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp = ClientMessage.temp(1L, new byte[] {1});
		_sendMessage(outbound, temp);
		ClientResponse received = _readResponse(outbound);
		ClientResponse committed = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.RECEIVED, received.type);
		Assert.assertEquals(ClientResponseType.COMMITTED, committed.type);
		Assert.assertEquals(0L, received.lastCommitGlobalOffset);
		Assert.assertEquals(1L, committed.lastCommitGlobalOffset);
		
		outbound.close();
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testSimulatedClientReconnect() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		
		// HACK - wait for startup.
		Thread.sleep(500);
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		UUID clientId = UUID.randomUUID();
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp1 = ClientMessage.temp(1L, new byte[] {1});
		ClientMessage temp2 = ClientMessage.temp(2L, new byte[] {2});
		ClientMessage temp3 = ClientMessage.temp(3L, new byte[] {3});
		_sendMessage(outbound, temp1);
		_readResponse(outbound);
		long lastCommitGlobalOffset = _readResponse(outbound).lastCommitGlobalOffset;
		// We will fake that 2 and 3 went missing.
		_sendMessage(outbound, temp2);
		_readResponse(outbound);
		_readResponse(outbound);
		_sendMessage(outbound, temp3);
		_readResponse(outbound);
		_readResponse(outbound);
		outbound.close();
		
		outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		_sendMessage(outbound, ClientMessage.reconnect(temp2.nonce, clientId, lastCommitGlobalOffset));
		// We expect to see the received and committed of temp2 and temp3.
		_readResponse(outbound);
		_readResponse(outbound);
		_readResponse(outbound);
		_readResponse(outbound);
		// Followed by the ready.
		ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp4 = ClientMessage.temp(4L, new byte[] {4});
		_sendMessage(outbound, temp4);
		_readResponse(outbound);
		ClientResponse commit4 = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.COMMITTED, commit4.type);
		Assert.assertEquals(temp4.nonce, commit4.nonce);
		// Since this is a commit and we sent 4 messages, we must see a final commit of 4.
		Assert.assertEquals(4L, commit4.lastCommitGlobalOffset);
		
		feeder.println("stop");
		runner.join();
	}

	@Test
	public void testSimplePoisonCase() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		PipedOutputStream outStream = new PipedOutputStream();
		PipedInputStream inStream = new PipedInputStream(outStream);
		PrintStream feeder = new PrintStream(outStream);
		System.setIn(inStream);
		
		// We need to run the Laminar process in a thread it will control and then sleep for startup.
		// (this way of using sleep for timing is a hack but this will eventually be made into a more reliable integration test, probably outside of JUnit).
		Thread runner = new Thread() {
			@Override
			public void run() {
				Laminar.main(new String[] {"--client", "2002", "--cluster", "2003", "--data", "/tmp/laminar"});
			}
		};
		runner.start();
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 21);
		beforeListener.setName("Before");
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Send 10 messages, then a poison, then another 10 messages.  After, wait for them all to commit.
			ClientResult[] results = new ClientResult[21];
			for (int i = 0; i < 10; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			results[10] = client.sendPoison(new byte[] {10});
			for (int i = 11; i < results.length; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForReceived();
				results[i].waitForCommitted();
			}
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 21);
		afterListener.setName("After");
		afterListener.start();
		
		// Wait for the listeners to stop and then verify what they found is correct.
		EventRecord[] beforeEvents = beforeListener.waitForTerminate();
		EventRecord[] afterEvents = afterListener.waitForTerminate();
		for (int i = 0; i < beforeEvents.length; ++i) {
			// Check the after, first, since that happened after the poison was done and the difference can point to different bugs.
			Assert.assertEquals(i, afterEvents[i].payload[0]);
			Assert.assertEquals(i, beforeEvents[i].payload[0]);
		}
		
		// Shut down.
		feeder.println("stop");
		runner.join();
	}


	private void _sendMessage(SocketChannel socket, ClientMessage message) throws IOException {
		byte[] serialized = message.serialize();
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		buffer.putShort((short)serialized.length);
		buffer.flip();
		int written = socket.write(buffer);
		Assert.assertEquals(buffer.capacity(), written);
		buffer = ByteBuffer.wrap(serialized);
		written = socket.write(buffer);
		Assert.assertEquals(buffer.capacity(), written);
	}

	private ClientResponse _readResponse(SocketChannel socket) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		int read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		buffer.flip();
		int size = Short.toUnsignedInt(buffer.getShort());
		buffer = ByteBuffer.allocate(size);
		read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		return ClientResponse.deserialize(buffer.array());
	}


	private static class ListenerThread extends Thread {
		private final byte[] _message;
		private final CountDownLatch _latch;
		public final ListenerConnection listener;
		
		public ListenerThread(InetSocketAddress address, byte[] message, CountDownLatch latch) throws IOException {
			_message = message;
			_latch = latch;
			// We want to expose the connection so tests can request it shut down.
			this.listener = ListenerConnection.open(address, 0L);
		}
		
		@Override
		public void run() {
			try {
				EventRecord record = this.listener.pollForNextEvent();
				if (null == _message) {
					// We expected failure.
					Assert.assertNull(record);
				} else {
					// We only expect the one.
					Assert.assertEquals(1L, record.localOffset);
					Assert.assertArrayEquals(_message, record.payload);
				}
				_latch.countDown();
				// ListenerConnection is safe against redundant closes (even though the underlying NetworkManager is not).
				// The reasoning behind this is that ListenerConnection is simpler and is directly accessed by client code.
				this.listener.close();
			} catch (IOException | InterruptedException e) {
				Assert.fail(e.getLocalizedMessage());
			}
		}
	}


	private static class CaptureListener extends Thread {
		private final ListenerConnection _listener;
		private final EventRecord[] _captured;
		
		public CaptureListener(InetSocketAddress address, int messagesToCapture) throws IOException {
			_listener = ListenerConnection.open(address, 0L);
			_captured = new EventRecord[messagesToCapture];
		}
		
		public EventRecord[] waitForTerminate() throws InterruptedException {
			this.join();
			return _captured;
		}
		
		@Override
		public void run() {
			try {
				for (int i = 0; i < _captured.length; ++i) {
					_captured[i] = _listener.pollForNextEvent();
					Assert.assertEquals(i + 1, _captured[i].localOffset);
				}
				_listener.close();
			} catch (IOException | InterruptedException e) {
				Assert.fail(e.getLocalizedMessage());
			}
		}
	}
}
