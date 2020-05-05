package com.jeffdisher.laminar.state;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;


/**
 * Unit tests for NodeState:  The core decision-making component of Laminar.
 */
public class TestNodeState {
	/**
	 * Tests that attempting to start up a node before registering all the components in as assertion failure.
	 */
	@Test(expected=AssertionError.class)
	public void testMissingPieces() throws Throwable {
		NodeState state = new NodeState(_createConfig());
		state.runUntilShutdown();
	}


	private static ClusterConfig _createConfig() {
		return ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(new InetSocketAddress(1), new InetSocketAddress(2))});
	}
}
