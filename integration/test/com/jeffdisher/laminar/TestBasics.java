package com.jeffdisher.laminar;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetAddress;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Integration tests of the basic behaviour and life-cycle.
 * These are quick and basically are just for testing things like command-line processing and proper start-up/shut-down.
 */
public class TestBasics {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testMissingArgs() throws Throwable {
		ByteArrayOutputStream err = new ByteArrayOutputStream(1024);
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperRaw(new String[] {"missing"}, err);
		int exit = wrapper.stop();
		Assert.assertEquals(1, exit);
		byte[] errBytes = err.toByteArray();
		String errorString = new String(errBytes);
		Assert.assertEquals("Fatal start-up error: Missing options!  Usage:  Laminar --clientIp <client_ip> --clientPort <client_port> --clusterIp <cluster_ip> --clusterPort <cluster_port> --data <data_directory_path>\n"
				, errorString);
	}

	@Test
	public void testNormalRun() throws Throwable {
		// We just want this to start everything and then shut down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testNormalRun", 2001, 2000, _folder.newFolder());
		int exit = wrapper.stop();
		Assert.assertEquals(0, exit);
	}

	@Test
	public void testIpv4() throws Throwable {
		// We just want this to start everything and then shut down with a specific IPv4 address.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperWithUuidAndIp("testIpv4", UUID.randomUUID(), "127.0.0.1", 2001, 2000, _folder.newFolder());
		int exit = wrapper.stop();
		Assert.assertEquals(0, exit);
	}

	@Test
	public void testIpv6() throws Throwable {
		// We just want this to start everything and then shut down with a specific IPv4 address.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperWithUuidAndIp("testIpv6", UUID.randomUUID(), "::1", 2001, 2000, _folder.newFolder());
		int exit = wrapper.stop();
		Assert.assertEquals(0, exit);
	}

	/**
	 * Tests that a single node will fail to restart if it is given a mismatched UUID.
	 */
	@Test
	public void testRestartUuidMismatch() throws Throwable {
		// We want to reuse the data directory so we see restart logic.
		File dataDirectory = _folder.newFolder();
		UUID serverUuid = UUID.randomUUID();
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperWithUuid("testRestartUuidMismatch-PRE", serverUuid, 2000, 3000, dataDirectory);
		Assert.assertEquals(0, wrapper.stop());
		
		String localhost = InetAddress.getLocalHost().getHostAddress();
		wrapper = ServerWrapper.startedServerWrapperRaw(new String[] {"--uuid", UUID.randomUUID().toString()
				, "--clusterIp", localhost
				, "--clusterPort", Integer.toString(2000)
				, "--clientIp", localhost
				, "--clientPort", Integer.toString(3000)
				, "--data", dataDirectory.getAbsolutePath()
				}, new ByteArrayOutputStream());
		Assert.assertEquals(1, wrapper.stop());
		
		// But the correct UUID will work.
		wrapper = ServerWrapper.startedServerWrapperWithUuid("testRestartUuidMismatch-POST", serverUuid, 2000, 3000, dataDirectory);
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}
}
