package com.jeffdisher.laminar.network;

import java.nio.channels.ServerSocketChannel;


/**
 * Top-level abstraction of a collection of network connections related to interactions with clients.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 */
public class ClientManager {
	public ClientManager(ServerSocketChannel clientSocket, IClientManagerBackgroundCallbacks callbackTarget) {
		// TODO Auto-generated constructor stub
	}

	public void startAndWaitForReady() {
		// TODO Auto-generated method stub
		
	}

	public void stopAndWaitForTermination() {
		// TODO Auto-generated method stub
		
	}
}
