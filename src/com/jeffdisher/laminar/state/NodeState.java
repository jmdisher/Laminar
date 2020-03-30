package com.jeffdisher.laminar.state;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.network.NetworkManager;


/**
 * Maintains the state of this specific node.
 * Primarily, this is where the main coordination thread sleeps until events are handed off to it.
 */
public class NodeState {
	public NodeState(NetworkManager clientManager, NetworkManager clusterManager, DiskManager disk, ConsoleManager console) {
		// TODO Auto-generated constructor stub
	}

	public void runUntilShutdown() {
		// TODO Auto-generated method stub
		
	}
}
