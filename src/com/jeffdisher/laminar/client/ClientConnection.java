package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientResponse;
import com.jeffdisher.laminar.network.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.NetworkManager;
import com.jeffdisher.laminar.network.NetworkManager.NodeToken;
import com.jeffdisher.laminar.utils.Assert;


/**
 * The client connection and fail-over handling logic requires some explanation:
 * -when a new client connects, it sends a CONNECT message, with a version number
 * -the server response with CONNECT_OK, and provides its current committed offset
 * -the client sends (SEND) messages with its self-assigned UUID and a monotonically increasing nonce (1-indexed)
 * -the server will respond (ACK) with the nonce, the corresponding global event offset of this message, and its current
 *  committed offset
 * -once the server has decided to commit the message and run it, it will send a COMMIT with the nonce, corresponding
 *  result state (always SUCCESS except in the case of AVM execution which may be FAILED or EXCEPTION), and its current
 *  committed offset
 * -a server can, at any time, send a response of REDIRECT with the leader IP/port if it isn't the leader
 * 
 * -a server in CANDIDATE state will not send any responses to clients until its state is settled
 * 
 * -when an existing client reconnects, it sends a RECONNECT message, with a version number, its existing UUID, and the
 *  last known commit offset from the server
 * -the server responds with a sequence of DURABLE messages with the nonce, corresponding global event offset, and
 *  result state of any committed messages which occur after the given commit offset (sent in-order)
 * -the server then sends a sequence of PENDING message with the nonce, and corresponding global event offset for any
 *  messages after that which are still not yet committed (they will be communicated using normal COMMIT messages,
 *  later)
 * -once these streams have been sent, the server will send a RECONNECT_DONE message, providing its current committed
 *  offset
 * To be clear:  These messages returned are only for messages with matching client UUIDs.
 * 
 * This handles both the cases of commits the client didn't find out about but also allows it to sync up its local nonce
 * and decide to resend messages which the cluster never received (even if the defunct leader saw the messages, the
 * client now knows the new leader didn't).
 * While this creates a potentially expensive "re-sync" kind of operation in the reconnect critical path, it avoids the
 * server needing any knowledge of client lifecycle or needing some kind of "re-send buffer and timeout", which adds
 * complexity and has no complete solutions.
 * In the future, in order to avoid erroneous use of this facility wasting server resources, a REJECT_CLIENT message may
 * be created so a server can ignore requests which would require reading through large amounts of data.  The core of
 * the design doesn't depend on this, however, so it won't be added in the first version.
 */
public class ClientConnection implements Closeable, INetworkManagerBackgroundCallbacks {
	public static ClientConnection open(InetSocketAddress server) throws IOException {
		if (null == server) {
			throw new IllegalArgumentException("Address cannot be null");
		}
		ClientConnection connection = new ClientConnection(server);
		connection._network.startAndWaitForReady("ClientConnection");
		connection._network.createOutgoingConnection(server);
		return connection;
	}


	private final InetSocketAddress _serverAddress;
	private final NetworkManager _network;
	private final UUID _clientId;
	private NodeToken _connection;

	private volatile boolean _keepRunning;
	private Thread _backgroundThread;
	private int _pendingMessages;
	private boolean _canWrite;
	private final List<ClientResult> _outgoingMessages;
	// _inFlightMessages must be sorted since we traverse the keys in-order at the start and end of reconnect.
	// NOTE:  _inFlightMessages can ONLY be accessed by _backgroundThread.
	private final SortedMap<Long, ClientResult> _inFlightMessages;
	// This is part of the connection state machine - once a connection is established, we need to ask the internal
	// thread to send a handshake.  This also means that a single write is allowed, although _canWrite wasn't set for
	// general use.
	private boolean _handshakeRequired;
	// We keep track of whether or not we have received the CLIENT_READY message so callers can ask if the network is
	// "up" and to ensure we don't write while still waiting for state sync.
	private boolean _isClientReady;
	private long _nextNonce;
	// We store the last global commit the server sends us in responses so we can ask what happened since then, when
	// reconnecting.  It is only read or written by _backgroundThread.
	private long _lastCommitGlobalOffset;

	// Due to reconnection requirements, it is possible to fail a connection but not want to bring down the system.
	// Therefore, we will continue reconnection attempts, until told to close.  Unless we have an active connection, we
	// store a reference to the most recent failure we observed during connection, for external informational purposes.
	private IOException _currentConnectionFailure;
	private IOException _mostRecentConnectionFailure;
	// (track whether or not we ever disconnect in order to determine if the next connect is a new connect or reconnect)
	private boolean _hasDisconnectedEver;

	private ClientConnection(InetSocketAddress server) throws IOException {
		_serverAddress = server;
		_network = NetworkManager.outboundOnly(this);
		_clientId = UUID.randomUUID();
		_outgoingMessages = new LinkedList<>();
		_inFlightMessages = new TreeMap<>();
		// First message has nonce of 1L.
		_nextNonce = 1L;
		_keepRunning = true;
		_backgroundThread = new Thread() {
			@Override
			public void run() {
				_backgroundThreadMain();
			}
		};
		// Note that it is normally poor form to start the thread in the constructor but this is a private constructor
		// so the matter is 6 vs 1/2 dozen and this is more direct.
		_backgroundThread.setName("Laminar client (" + server + ")");
		_backgroundThread.start();
	}

	public synchronized void waitForConnection() throws InterruptedException {
		// Wait until we get a CLIENT_READY message on this connection.
		while (!_isClientReady) {
			this.wait();
		}
	}

	public synchronized ClientResult sendTemp(byte[] payload) {
		ClientMessage message = ClientMessage.temp(_nextNonce++, payload);
		ClientResult result = new ClientResult(message);
		_outgoingMessages.add(result);
		this.notifyAll();
		return result;
	}

	/**
	 * Every client has a UUID and this is the same after reconnections but new client instances always start with a new
	 * one.
	 * 
	 * @return The UUID of this client.
	 */
	public UUID getClientId() {
		return _clientId;
	}

	/**
	 * Allows the client code to check the current state of the connection to the cluster.
	 * This is required since the connection normally tries to reestablish itself, when a connection drops or can't be
	 * created.
	 * In some cases of client misconfiguration, a total cluster failure, or a serious network problem, this may result
	 * in clients freezing for non-obvious reasons.  This method exists to allow a view into that state.
	 * This is similar to waitForConnection() but it blocks while this method would just return false (or throw).  The
	 * main distinction is in use-case:  waitForConnection() is useful for bootstrapping client logic while this is more
	 * about an informational health check if something is wrong at the network level.
	 * 
	 * @return True if the client believes that a network connection exists.  False if a reconnection is in progress.
	 * @throws IOException If the connection or reconnection has been failing, this is the last error observed.
	 */
	public synchronized boolean checkConnection() throws IOException {
		boolean isNetworkUp = _isClientReady;
		if (!isNetworkUp && (null != _mostRecentConnectionFailure)) {
			throw _mostRecentConnectionFailure;
		}
		return isNetworkUp;
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
		// Clear any now-stale connection error.
		_mostRecentConnectionFailure = null;
		// We immediately send our handshake and wait for CLIENT_READY (so we don't set the _canWrite since claim it).
		_handshakeRequired = true;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeDisconnected(NodeToken node) {
		Assert.assertTrue(_connection == node);
		_connection = null;
		// We will also need to reconnect and wait for CLIENT_READY so the client isn't ready.
		_isClientReady = false;
		// We record that a disconnect happened so our next connection attempt is a reconnect.
		_hasDisconnectedEver = true;
		// TODO:  Find a way to observe the disconnect or report the exception, consistently.
		_currentConnectionFailure = new IOException("Closed");
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeConnectionFailed(NodeToken token, IOException cause) {
		Assert.assertTrue(null == _connection);
		// Store the cause and interrupt the background thread so it will attempt a reconnect.
		_currentConnectionFailure = cause;
		this.notifyAll();
	}
	// </INetworkManagerBackgroundCallbacks>

	@Override
	public void close() throws IOException {
		synchronized (this) {
			_keepRunning = false;
			this.notifyAll();
		}
		try {
			_backgroundThread.join();
		} catch (InterruptedException e) {
			// We don't rely on interruption.
			Assert.unexpected(e);
		}
		_network.stopAndWaitForTermination();
	}


	private void _backgroundThreadMain() {
		while (_keepRunning) {
			synchronized (this) {
				// Wait for something to do.
				// Note that we don't consider that we have anything to write until we have something in the outgoing list and the writable flag is set and the client is ready.
				while (_keepRunning && (0 == _pendingMessages) && (_outgoingMessages.isEmpty() || !_canWrite || !_isClientReady) && (null == _currentConnectionFailure) && !_handshakeRequired) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						// We don't rely on interruption.
						Assert.unexpected(e);
					}
				}
				
				// Interpret why we were unblocked.
				boolean canRead = false;
				ClientMessage handshakeToSend = null;
				ClientResult messageToWrite = null;
				if (_pendingMessages > 0) {
					_pendingMessages -= 1;
					canRead = true;
				}
				if (!_outgoingMessages.isEmpty() && _canWrite && _isClientReady) {
					// Currently, we only send a single message at a time but this will be made more aggressive in the future.
					messageToWrite = _outgoingMessages.remove(0);
					_canWrite = false;
				}
				if (null != _currentConnectionFailure) {
					// This happens when a connect or reconnect failure was observed.  Due to reconnection requirements,
					// we just want to try again until this succeeds or we are told to close.
					// We consume the connection failure and move it to _mostRecentConnectionFailure so it is available
					// for the client code's information.
					
					// There can be no current connection if there is a connection failure.
					Assert.assertTrue(null == _connection);
					try {
						_network.createOutgoingConnection(_serverAddress);
					} catch (IOException e) {
						// We are just restarting something we already did so a failure here would mean something big changed.
						throw Assert.unexpected(e);
					}
					_mostRecentConnectionFailure = _currentConnectionFailure;
					_currentConnectionFailure = null;
				}
				if (_handshakeRequired) {
					// If we have ever been disconnected, this is a reconnect, otherwise the _outgoing messages are just the client jumping the gun.
					if (_hasDisconnectedEver) {
						// Reconnect.
						// We need to send the lowest nonce we don't yet know committed (if any past this did commit, the server will update its state for us when it synthesizes the commits).
						// Note that, if there are no in-flight messages, this is a degenerate reconnect where the server will send us nothing.
						long lowestPossibleNextNonce = _inFlightMessages.isEmpty()
								? _nextNonce
								: _inFlightMessages.firstKey();
						handshakeToSend = ClientMessage.reconnect(lowestPossibleNextNonce, _clientId, _lastCommitGlobalOffset);
					} else {
						// New connection.
						handshakeToSend = ClientMessage.handshake(_clientId);
					}
					_handshakeRequired = false;
				}
				
				// Now apply whatever changes we can make in response to waking.
				if (canRead) {
					// Note that this method may receive a CLIENT_READY message in which case we may need to notify the user thread that the connection is up.
					boolean clientBecameReady = _lockedBackgroundReadAndDispatchMessage();
					if (clientBecameReady) {
						_isClientReady = true;
						this.notifyAll();
					}
				}
				if (null != messageToWrite) {
					_lockedBackgroundSendMessageInWrapper(messageToWrite);
				}
				if (null != handshakeToSend) {
					_lockedBackgroundSerializeAndSendMessage(handshakeToSend);
				}
			}
		}
	}

	private boolean _lockedBackgroundReadAndDispatchMessage() {
		byte[] message = _network.readWaitingMessage(_connection);
		// We were told this was here so it can't fail.
		Assert.assertTrue(null != message);
		
		boolean clientBecameReady = false;
		// Decode this.
		ClientResponse deserialized = ClientResponse.deserialize(message);
		// Update the global commit offset.
		// This number can stay the same or increase, but a decrease means the cluster is horribly broken (or is a different cluster).
		if (deserialized.lastCommitGlobalOffset < this._lastCommitGlobalOffset) {
			throw Assert.unimplemented("Determine how to handle the case of the server giving us a dangerously wrong answer");
		}
		this._lastCommitGlobalOffset = deserialized.lastCommitGlobalOffset;
		// Find the corresponding in-flight message and set its state.
		ClientResult result = _inFlightMessages.get(deserialized.nonce);
		switch (deserialized.type) {
		case INVALID:
			Assert.unreachable("Invalid response type");
			break;
		case ERROR:
			Assert.unimplemented("TODO:  Determine how to handle ERROR responses");
			break;
		case CLIENT_READY:
			// This means that we are able to start sending normal messages.
			// Between the RECONNECT and this message, the server re-sent synthesized received and committed messsages
			// from the lastCommitGlobalOffset we set them so they know the nonce of the next message we will send.
			// Now, all we have to do is take any remaining in-flight messages (those not satisfied by the sythesized
			// messages) and push them onto the front of outgoing messages, in the correct nonce order.  Then we can set
			// _isClientReady and the normal logic should play out as though nothing was wrong.
			
			// We rely on the SortedMap returning a sorted collection of values whose iterator returns them in ascending order of corresponding keys.
			_outgoingMessages.addAll(0, _inFlightMessages.values());
			_inFlightMessages.clear();
			clientBecameReady = true;
			break;
		case RECEIVED:
			result.setReceived();
			break;
		case COMMITTED:
			result.setCommitted();
			// If the message has committed, we will no longer re-send it.
			_inFlightMessages.remove(deserialized.nonce);
			break;
		default:
			Assert.unreachable("Default response case reached");
			break;
		}
		return clientBecameReady;
	}

	private void _lockedBackgroundSendMessageInWrapper(ClientResult wrapper) {
		// Unwrap the message.
		ClientMessage messageToWrite = wrapper.message;
		_lockedBackgroundSerializeAndSendMessage(messageToWrite);
		// We also need to track this as an in-flight message.
		_inFlightMessages.put(messageToWrite.nonce, wrapper);
	}

	private void _lockedBackgroundSerializeAndSendMessage(ClientMessage messageToWrite) {
		// Serialize the message.
		byte[] serialized = messageToWrite.serialize();
		boolean didSend = _network.trySendMessage(_connection, serialized);
		// We only wrote in response to the buffer being empty so this can't fail.
		Assert.assertTrue(didSend);
	}
}
