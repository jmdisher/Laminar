package com.jeffdisher.laminar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;


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
		
		// HACK:  Wait for start.
		Thread.sleep(2000);
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress("localhost", 2002))) {
			ClientResult result = client.sendTemp("Hello World!".getBytes());
			result.waitForReceived();
			result.waitForCommitted();
		}
		feeder.println("stop");
		runner.join();
	}
}
