package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
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
 * If the user needs to restart the ListenerConnection, at a later time, they will need to provide the localOffset of
 * the last event returned in order to ensure a seamless continuation of events.
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
	 * @param previousLocalOffset The most recently returned local offset from a previous listener connection (0L if
	 * this is a new listener with no existing state).
	 * @return An initialized, but not yet connected, listener.
	 * @throws IOException Something went wrong in initializing the network layer.
	 */
	public static ListenerConnection open(InetSocketAddress server, long previousLocalOffset) throws IOException {
		if (null == server) {
			throw new IllegalArgumentException("Address cannot be null");
		}
		ListenerConnection connection = new ListenerConnection(server, previousLocalOffset);
		connection._network.startAndWaitForReady("ListenerConnection");
		connection._network.createOutgoingConnection(server);
		return connection;
	}


	private final InetSocketAddress _serverAddress;
	private final NetworkManager _network;
	private NetworkManager.NodeToken _connection;
	// We also track the latest config from the cluster - this is currently just used for testing but will eventually be used to govern reconnect decisions.
	private ClusterConfig _currentClusterConfig;

	private boolean _keepRunning;
	// Since we are using the user's thread to perform the poll and we must stop polling before stopping the network
	// so we set a flag when the user's thread is in the polling loop in order to coordinate the shutdown properly.
	// (if this were our own thread, we would join, instead).
	private boolean _isPollActive;
	private boolean _didSendListen;
	private int _pendingMessages;
	private long _previousLocalOffset;

	// Due to reconnection requirements, it is possible to fail a connection but not want to bring down the system.
	// Therefore, we will continue reconnection attempts, until told to close.  Unless we have an active connection, we
	// store a reference to the most recent failure we observed during connection, for external informational purposes.
	private IOException _currentConnectionFailure;
	private IOException _mostRecentConnectionFailure;

	private ListenerConnection(InetSocketAddress server, long previousLocalOffset) throws IOException {
		_serverAddress = server;
		_network = NetworkManager.outboundOnly(this);
		_keepRunning = true;
		// We "sent" listen until a new connection opens and then it is set to false.
		_didSendListen = true;
		_pendingMessages = 0;
		_previousLocalOffset = previousLocalOffset;
	}

	/**
	 * Block until the next EventRecord is available, then decode and return it.
	 * Note that this call is interruptable using a thread interrupt.
	 * 
	 * @return The next EventRecord or null if the receiver was shut down.
	 * @throws InterruptedException If the calling thread received an interrupt.
	 */
	public synchronized EventRecord pollForNextEvent() throws InterruptedException {
		EventRecord record = null;
		_isPollActive = true;
		try {
			boolean tryAgain = true;
			while (tryAgain) {
				// We only try again if we keep getting config changes.
				tryAgain = false;
				record = _doLockedPollForNextEvent(_previousLocalOffset);
				// Note that we don't want to progress if we couldn't connect.
				if (null != record) {
					// We want to intercept any CONFIG_CHANGE events to update our connection state, instead of returning this to the user (since it is a control message, not data).
					if (EventRecordType.CONFIG_CHANGE == record.type) {
						// We just set this as our config in case we need it for reconnect.
						_currentClusterConfig = ClusterConfig.deserialize(record.payload);
						tryAgain = true;
					} else {
						_previousLocalOffset = record.localOffset;
					}
				}
			}
		} finally {
			_isPollActive = false;
			// If we are shutting down, notify anyone who might be waiting for us to rationalize the state of these flags.
			if (!_keepRunning) {
				this.notifyAll();
			}
		}
		return record;
	}

	/**
	 * Allows the client code to check the current state of the connection to the cluster.
	 * This is required since the connection normally tries to reestablish itself, when a connection drops or can't be
	 * created.
	 * In some cases of listener misconfiguration, a total cluster failure, or a serious network problem, this may
	 * result in listeners running silent when they actually should be seeing new events.  This method exists to allow a
	 * view into that state.
	 * 
	 * @return True if the listener believes that a network connection exists.  False if a reconnection is in progress.
	 * @throws IOException If the connection or reconnection has been failing, this is the last error observed.
	 */
	public synchronized boolean checkConnection() throws IOException {
		boolean isNetworkUp = (null != _connection);
		if (!isNetworkUp && (null != _mostRecentConnectionFailure)) {
			throw _mostRecentConnectionFailure;
		}
		return isNetworkUp;
	}

	/**
	 * @return The most recent cluster config we received from the server (null during start-up).
	 */
	public ClusterConfig getCurrentConfig() {
		return _currentClusterConfig;
	}

	// <INetworkManagerBackgroundCallbacks>
	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(_connection == node);
		// We don't do anything with this message (might in the future).
	}

	@Override
	public synchronized void nodeReadReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(_connection == node);
		_pendingMessages += 1;
		if (1 == _pendingMessages) {
			this.notifyAll();
		}
	}

	@Override
	public synchronized void outboundNodeConnected(NetworkManager.NodeToken node) {
		Assert.assertTrue(null == _connection);
		_connection = node;
		// Reset our need to send the listen.
		_didSendListen = false;
		// Clear any now-stale connection error.
		_mostRecentConnectionFailure = null;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(_connection == node);
		_connection = null;
		// Reset our status to waiting for a connection.
		_didSendListen = true;
		_pendingMessages = 0;
		_currentConnectionFailure = cause;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
		Assert.assertTrue(null == _connection);
		_currentConnectionFailure = cause;
		this.notifyAll();
	}
	// </INetworkManagerBackgroundCallbacks>

	@Override
	public void close() throws IOException {
		boolean shouldStopNetwork = false;
		boolean interrupt = false;
		synchronized (this) {
			// We only proceed to stop this if nobody else already did.
			shouldStopNetwork = _keepRunning;
			if (shouldStopNetwork) {
				_keepRunning = false;
				this.notifyAll();
				while (_isPollActive) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						// This operation isn't interruptable but we will restore the state when done.
						interrupt = true;
					}
				}
			}
		}
		// Only the thread which transitioned the state to no longer running should stop the network since only 1 thread is allowed to.
		if (shouldStopNetwork) {
			_network.stopAndWaitForTermination();
		}
		// If someone tried to interrupt us during this uninterruptable operation, re-interrupt the thread.
		if (interrupt) {
			Thread.currentThread().interrupt();
		}
	}


	private EventRecord _doLockedPollForNextEvent(long previousLocalOffset) throws InterruptedException, AssertionError {
		EventRecord record = null;
		while (_keepRunning && (null == record)) {
			// Wait until we are ready to take some action.  Cases to exit:
			// -told to stop (!_keepRunning)
			// -we haven't yet sent the "listen" message on a new connection
			// -connection is open and we haven't sent the listen
			// -there is a connection failure
			// -we have pending messages to send
			while (_keepRunning && _didSendListen && (null == _currentConnectionFailure) && (0 == _pendingMessages)) {
				this.wait();
			}
			
			if (!_didSendListen) {
				Assert.assertTrue(null != _connection);
				// The connection opened but we haven't send the listen message.
				ClientMessage listen = ClientMessage.listen(previousLocalOffset);
				boolean didSend = _network.trySendMessage(_connection, listen.serialize());
				// We had the _canWrite, so this can't fail.
				Assert.assertTrue(didSend);
				_didSendListen = true;
			}
			if (null != _currentConnectionFailure) {
				// The connection failed - save the failure and restart the connection attempt.
				// (in the future, this will be rolled into broader reconnect logic).
				try {
					_network.createOutgoingConnection(_serverAddress);
				} catch (IOException e) {
					// We are just restarting something we already did so a failure here would mean something big changed.
					throw Assert.unexpected(e);
				}
				_mostRecentConnectionFailure = _currentConnectionFailure;
				_currentConnectionFailure = null;
			}
			if (_pendingMessages > 0) {
				Assert.assertTrue(null != _connection);
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
}
