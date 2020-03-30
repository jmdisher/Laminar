package com.jeffdisher.laminar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


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
		Laminar.main(new String[] {"--client", "2000", "--cluster", "2001", "--data", "/tmp/laminar"});
	}
}
