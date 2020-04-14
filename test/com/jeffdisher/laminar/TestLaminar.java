package com.jeffdisher.laminar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.EventRecord;


/**
 * It doesn't make sense to test the entire system from within a unit test but this gives a quick way of verifying the
 * top-level system is correct before fleshing out the actual function units.
 * This will be replaced with some kind of integration test, later on.
 */
class TestLaminar {
	private InputStream realIn;
	private PrintStream realOut;
	private PrintStream realErr;

	private ByteArrayOutputStream out;
	private ByteArrayOutputStream err;

	@BeforeEach
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

	@AfterEach
	public void afterEach() {
		System.setIn(this.realIn);
		System.setOut(this.realOut);
		System.setErr(this.realErr);
	}

	@Test
	void testMissingArgs() {
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
	void testNormalRun() {
		// We just want this to start everything and then shut down.
		System.setIn(new ByteArrayInputStream("stop\n".getBytes()));
		Laminar.main(new String[] {"--client", "2000", "--cluster", "2001", "--data", "/tmp/laminar"});
	}

	@Test
	void testSimpleClient() throws Throwable {
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
	void testSimpleClientAndListeners() throws Throwable {
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
	void testClientForceDisconnect() throws Throwable {
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
	void testClientFailedConnection() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		InetSocketAddress address = new InetSocketAddress("localhost", 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Even though we aren't yet connected, the client can technically still attempt to send messages.
			client.sendTemp(message);
			// HACK:  Wait for the connection to fail.
			Thread.sleep(500);
			Assertions.assertThrows(IOException.class, () -> {
				client.checkConnection();
			});
		}
	}


	private static class ListenerThread extends Thread {
		private final InetSocketAddress _address;
		private final byte[] _message;
		private final CountDownLatch _latch;
		
		public ListenerThread(InetSocketAddress address, byte[] message, CountDownLatch latch) {
			_address = address;
			_message = message;
			_latch = latch;
		}
		
		@Override
		public void run() {
			try(ListenerConnection listener = ListenerConnection.open(_address)) {
				EventRecord record = listener.pollForNextEvent(0L);
				// We only expect the one.
				Assert.assertEquals(1L, record.localOffset);
				Assert.assertArrayEquals(_message, record.payload);
				_latch.countDown();
			} catch (IOException | InterruptedException e) {
				Assert.fail(e.getLocalizedMessage());
			}
		}
	}
}
