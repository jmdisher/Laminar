package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
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


	private InetSocketAddress _serverAddress;
	private final NetworkManager _network;
	private final UUID _clientId;
	private NetworkManager.NodeToken _connection;
	// We also track the latest config from the cluster - this is currently just used for testing but will eventually be used to govern reconnect decisions.
	private ClusterConfig _currentClusterConfig;

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
		// Make sure we erase the hostname from this, if we were given one (fixes some checks, later on, since the internal system never uses hostnames).
		// This is largely gratuitous but makes some very precise testing possible.
		_serverAddress = new InetSocketAddress(InetAddress.getByAddress(server.getAddress().getAddress()), server.getPort());
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

	/**
	 * Blocks until the connection to the cluster is successfully established.
	 * This differs from waitForConnectionOrFailure() by ignoring errors which it would through, only returning in case
	 * of a connection or throwing in case of an interruption of the thread by user code.
	 * 
	 * @throws InterruptedException The user interrupted this thread before it had an answer.
	 */
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

	public synchronized ClientResult sendPoison(byte[] payload) {
		ClientMessage message = ClientMessage.poison(_nextNonce++, payload);
		ClientResult result = new ClientResult(message);
		_outgoingMessages.add(result);
		this.notifyAll();
		return result;
	}

	public synchronized ClientResult sendUpdateConfig(ClusterConfig config) {
		ClientMessage message = ClientMessage.updateConfig(_nextNonce++, config);
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
	 * @return The nonce that this client will assign to the next message it sends.
	 */
	public long getNextNonce() {
		return _nextNonce;
	}

	/**
	 * Allows the client code to check the current state of the connection to the cluster.
	 * This is required since the connection normally tries to reestablish itself, when a connection drops or can't be
	 * created.
	 * In some cases of client misconfiguration, a total cluster failure, or a serious network problem, this may result
	 * in clients freezing for non-obvious reasons.  This method exists to allow a view into that state.
	 * This method will block until the connection is up or until there is a connection error.
	 * The key difference between this and waitForConnection(), above, is that this will throw an error if a failure was
	 * observed and the connection isn't up while that method will ignore failures and block until the connection is up.
	 * 
	 * @throws IOException If the connection or reconnection has been failing, this is the last error observed.
	 * @throws InterruptedException The user interrupted this thread before it had an answer.
	 */
	public synchronized void waitForConnectionOrFailure() throws IOException, InterruptedException {
		while (!_isClientReady && (null == _mostRecentConnectionFailure)) {
			this.wait();
		}
		if (!_isClientReady) {
			throw _mostRecentConnectionFailure;
		}
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
	public synchronized void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(_connection == node);
		_canWrite = true;
		this.notifyAll();
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
		// Clear any now-stale connection error.
		_mostRecentConnectionFailure = null;
		// We immediately send our handshake and wait for CLIENT_READY (so we don't set the _canWrite since claim it).
		_handshakeRequired = true;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(_connection == node);
		_handleDisconnect(cause);
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
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
						// Unset any received flags in these since we don't actually know if the other side received them (not in the fail-over case, at least) and rely on the reconnect to fix state.
						for (ClientResult result : _inFlightMessages.values()) {
							result.clearReceived();
						}
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
			
			// The CLIENT_READY includes the cluster's active config so we want to read this and verify it is consistent.
			// (this is just a temporary check to verify that the ClusterConfig is working correctly - this will be removed once config management is fully implemented)
			ClusterConfig currentConfig = ClusterConfig.deserialize(deserialized.extraData);
			Assert.assertTrue(1 == currentConfig.entries.length);
			Assert.assertTrue(_serverAddress.equals(currentConfig.entries[0].client));
			// Whether this was null (start-up) or something else (if the config change mid-run), we want to set this as our active config in case of a reconnect.
			_currentClusterConfig = currentConfig;
			
			// We need to sift through the in-flight messages now that the reconnect is done:
			// -those RECEIVED can stay in in-flight
			// -those which have not been received need to be pushed back into the front of outgoing to be re-sent.
			
			// We rely on the SortedMap returning a sorted collection of values whose iterator returns them in ascending order of corresponding keys.
			boolean stillSearching = true;
			while (!_inFlightMessages.isEmpty() && stillSearching) {
				long keyToCheck = _inFlightMessages.lastKey();
				ClientResult backResult = _inFlightMessages.get(keyToCheck);
				if (backResult.isReceived()) {
					stillSearching = false;
				} else {
					_inFlightMessages.remove(keyToCheck);
					_outgoingMessages.add(0, backResult);
				}
			}
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
		case UPDATE_CONFIG:
			// This is an out-of-band config update.  This has a -1 nonce so make sure we didn't find a message.
			Assert.assertTrue(null == result);
			// Now, just deserialize and set the config (whether our exising one was null, or not).
			_currentClusterConfig = ClusterConfig.deserialize(deserialized.extraData);
			break;
		case REDIRECT:
			// We need to shut down our current connection and establish a new one to this new cluster leader.
			ConfigEntry newLeader = ConfigEntry.deserializeFrom(ByteBuffer.wrap(deserialized.extraData));
			// We will do this by changing the server address and simulating a dropped connection.
			_serverAddress = newLeader.client;
			_network.closeConnection(_connection);
			_handleDisconnect(new IOException("Redirect received"));
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

	private void _handleDisconnect(IOException cause) {
		_connection = null;
		// We will also need to reconnect and wait for CLIENT_READY so the client isn't ready.
		_isClientReady = false;
		// We record that a disconnect happened so our next connection attempt is a reconnect.
		_hasDisconnectedEver = true;
		_currentConnectionFailure = cause;
		// We also need to dump any pending messages we were told about since we set _connection to null.
		_pendingMessages = 0;
	}
}
