package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	// The handshake response is what the client waits on before sending messages.
	// (this is final until we implement the reconnect logic where this will be modified to wait for full reconnect)
	private final ClientResult _handshakeResult;
	private NodeToken _connection;

	private volatile boolean _keepRunning;
	private Thread _backgroundThread;
	private int _pendingMessages;
	private boolean _canWrite;
	private final List<ClientResult> _outgoingMessages;
	// NOTE:  _inFlightMessages can ONLY be accessed by _backgroundThread.
	private final Map<Long, ClientResult> _inFlightMessages;
	private long _nextNonce;

	// Due to reconnection requirements, it is possible to fail a connection but not want to bring down the system.
	// Therefore, we will continue reconnection attempts, until told to close.  Unless we have an active connection, we
	// store a reference to the most recent failure we observed during connection, for external informational purposes.
	private IOException _currentConnectionFailure;
	private IOException _mostRecentConnectionFailure;

	private ClientConnection(InetSocketAddress server) throws IOException {
		_serverAddress = server;
		_network = NetworkManager.outboundOnly(this);
		_clientId = UUID.randomUUID();
		_outgoingMessages = new LinkedList<>();
		_inFlightMessages = new HashMap<>();
		_nextNonce = 0L;
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
		
		// (we will always queue up our handshake since this is a new client connection).
		ClientMessage handshake = ClientMessage.handshake(_nextNonce++, _clientId);
		_handshakeResult = new ClientResult(handshake);
		_outgoingMessages.add(_handshakeResult);
	}

	public void waitForConnection() throws InterruptedException {
		// We just wait for the result of the handshake (the commit is synthesized on the server so it is the same as received).
		_handshakeResult.waitForCommitted();
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
	 * 
	 * @return True if the client believes that a network connection exists.  False if a reconnection is in progress.
	 * @throws IOException If the connection or reconnection has been failing, this is the last error observed.
	 */
	public synchronized boolean checkConnection() throws IOException {
		boolean isNetworkUp = (null != _connection);
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
		// Every connection starts writable.
		_canWrite = true;
		this.notifyAll();
	}

	@Override
	public synchronized void outboundNodeDisconnected(NodeToken node) {
		Assert.assertTrue(_connection == node);
		_connection = null;
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
			boolean canRead = false;
			ClientResult messageToWrite = null;
			
			// Wait for something to do.
			synchronized (this) {
				while (_keepRunning && (0 == _pendingMessages) && (_outgoingMessages.isEmpty() || !_canWrite) && (null == _currentConnectionFailure)) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						// We don't rely on interruption.
						Assert.unexpected(e);
					}
				}
				if (_pendingMessages > 0) {
					_pendingMessages -= 1;
					canRead = true;
				}
				if (!_outgoingMessages.isEmpty() && _canWrite) {
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
			}
			
			// Do whatever we can.
			if (canRead) {
				_readAndDispatchMessage();
			}
			if (null != messageToWrite) {
				_serializeAndWriteMessage(messageToWrite);
			}
		}
	}

	private void _readAndDispatchMessage() {
		byte[] message = _network.readWaitingMessage(_connection);
		// We were told this was here so it can't fail.
		Assert.assertTrue(null != message);
		
		// Decode this.
		ClientResponse deserialized = ClientResponse.deserialize(message);
		// Find the corresponding in-flight message and set its state.
		ClientResult result = _inFlightMessages.get(deserialized.nonce);
		switch (deserialized.type) {
		case INVALID:
			Assert.unreachable("Invalid response type");
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
	}

	private void _serializeAndWriteMessage(ClientResult messageToWrite) {
		// Serialize the message.
		byte[] serialized = messageToWrite.message.serialize();
		boolean didSend = _network.trySendMessage(_connection, serialized);
		// We only wrote in response to the buffer being empty so this can't fail.
		Assert.assertTrue(didSend);
		// We also need to track this as an in-flight message.
		_inFlightMessages.put(messageToWrite.message.nonce, messageToWrite);
	}
}
