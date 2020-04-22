package com.jeffdisher.laminar.client;

import com.jeffdisher.laminar.types.ClientMessage;


/**
 * A ClientResult is used by client-side application code to track the progress of a message it sent as it progresses
 * through the cluster.
 * Specifically, the client can block on the cluster receiving the message and/or committing the message.
 */
public class ClientResult {
	public final ClientMessage message;
	private boolean _received;
	private boolean _committed;

	public ClientResult(ClientMessage message) {
		this.message = message;
	}

	public synchronized void waitForReceived() throws InterruptedException {
		while (!_received) {
			// We allow the user to interrupt their own thread.
			this.wait();
		}
	}

	public synchronized void waitForCommitted() throws InterruptedException {
		while (!_committed) {
			// We allow the user to interrupt their own thread.
			this.wait();
		}
	}

	public synchronized void setReceived() {
		_received = true;
		this.notifyAll();
	}

	public synchronized void setCommitted() {
		_committed = true;
		this.notifyAll();
	}
}
