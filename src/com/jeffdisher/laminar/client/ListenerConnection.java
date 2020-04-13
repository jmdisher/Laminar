package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.NetworkManager;
import com.jeffdisher.laminar.network.NetworkManager.NodeToken;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * The ListenerConnection is similar to the ClientConnection but far simpler.
 * It is read-only while ClientConnection is mostly write-only.
 * The fail-over logic is far simpler since there is no need to sync up the state of outgoing messages with the new
 * leader.  Instead, the last read request is just re-sent.
 * The basic logic of the listener is that it connects to a server and sends a LISTEN request, instead of a HANDSHAKE
 * request.
 * The listener never needs to send another message to the server and the server will send back raw EventRecord data
 * as quickly as it can, whenever new matching events are available.  Note that these are not in a message container,
 * only the framing facility of the network layer.
 * This means that the listener is responsible for telling the server the last local offset it received (both global and
 * local offsets are 1-indexed so the initial request passes 0 to request all events) and then it will receive updates
 * from that point onward.
 * NOTE:  "Local" offsets are the per-topic offsets while the "global" offsets are the input event offsets.  For a
 * single-topic, non-programmable system, they map 1-to-1.  The input events are split into per-topic events, when
 * committed, meaning they are given local offsets.  This, alone, would not be a reason to use this "local" addressing
 * mode.  The ultimate reason is due to programmable topics as they can produce 0 or many events in response to
 * processing an input event.
 * The user directly polls the connection for the next event received.  As the user owns this thread, it can send an
 * interrupt to break it out of its poll operation.
 * Note that the only internal thread this creates is to manage the network, not process the incoming data, so no
 * progress will be made while the user's thread isn't polling.
 */
public class ListenerConnection implements Closeable, INetworkManagerBackgroundCallbacks {
	/**
	 * Creates a new listener connection with a background connection attempt to server.
	 * 
	 * @param server The server to which the client should listen.
	 * @return An initialized, but not yet connected, listener.
	 * @throws IOException Something went wrong in initializing the network layer.
	 */
	public static ListenerConnection open(InetSocketAddress server) throws IOException {
		if (null == server) {
			throw new IllegalArgumentException("Address cannot be null");
		}
		ListenerConnection connection = new ListenerConnection();
		connection._network.startAndWaitForReady();
		connection._network.createOutgoingConnection(server);
		return connection;
	}


	private final NetworkManager _network;
	private NodeToken _connection;

	private volatile boolean _keepRunning;
	private boolean _canWrite;
	private boolean _didSendListen;
	private int _pendingMessages;

	private ListenerConnection() throws IOException {
		_network = NetworkManager.outboundOnly(this);
		_keepRunning = true;
		_canWrite = false;
		_didSendListen = false;
		_pendingMessages = 0;
	}

	/**
	 * Block until the next EventRecord is available, then decode and return it.
	 * Note that this call is interruptable using a thread interrupt.
	 * 
	 * @param previousLocalOffset The most recently returned local offset (0L if this is the first call).
	 * @return The next EventRecord or null if the receiver was shut down.
	 * @throws InterruptedException If the calling thread received an interrupt.
	 */
	public synchronized EventRecord pollForNextEvent(long previousLocalOffset) throws InterruptedException {
		// Wait until we are ready to take some action.
		while (_keepRunning && !_canWrite && !_didSendListen) {
			this.wait();
		}
		
		// See if we need to set up the connection.
		// Note:  Once reconnect logic is added, the interaction with setup and blocking read will need to change.
		if (!_didSendListen && _canWrite) {
			// Send the listen message.
			ClientMessage listen = ClientMessage.listen(previousLocalOffset);
			boolean didSend = _network.trySendMessage(_connection, listen.serialize());
			// We had the _canWrite, so this can't fail.
			Assert.assertTrue(didSend);
			// We reset these flags in case the user interrupts this and wants to resume, later.
			_didSendListen = true;
			_canWrite = false;
		}
		
		EventRecord record = null;
		while (_keepRunning && (null == record)) {
			while (_keepRunning && (0 == _pendingMessages)) {
				this.wait();
			}
			if (_pendingMessages > 0) {
				// Grab a message, decode it, and return it.
				byte[] message = _network.readWaitingMessage(_connection);
				// (we know this must have been available).
				Assert.assertTrue(null != message);
				_pendingMessages -= 1;
				
				// Currently, we only send a single EventRecord per payload, not in any message container.
				record = EventRecord.deserialize(message);
			}
		}
		return record;
	}

	// <INetworkManagerBackgroundCallbacks>
	@Override
	public void nodeDidConnect(NodeToken node) {
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public void nodeDidDisconnect(NodeToken node) {
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public synchronized void nodeWriteReady(NodeToken node) {
		Assert.assertTrue(_connection == node);
		// We still handle this flag, even thought it doesn't currently matter for the listener (it only sends one message).
		_canWrite = true;
		this.notifyAll();
	}

	@Override
	public synchronized void nodeReadReady(NodeToken node) {
		Assert.assertTrue(_connection == node);
		_pendingMessages += 1;
		if (1 == _pendingMessages) {
			this.notifyAll();
		}
	}

	@Override
	public synchronized void outboundNodeConnected(NodeToken node) {
		Assert.assertTrue(null == _connection);
		_connection = node;
		// Every connection starts writable.
		_canWrite = true;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeDisconnected(NodeToken node) {
		Assert.assertTrue(_connection == node);
		_connection = null;
	}
	// </INetworkManagerBackgroundCallbacks>

	@Override
	public void close() throws IOException {
		synchronized (this) {
			_keepRunning = false;
			this.notifyAll();
		}
		_network.stopAndWaitForTermination();
	}
}
