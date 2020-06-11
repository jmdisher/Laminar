package com.jeffdisher.laminar.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.response.ClientResponse;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_ClusterConfig;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_Commit;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_ConfigEntry;
import com.jeffdisher.laminar.utils.Assert;
import com.jeffdisher.laminar.utils.UninterruptibleQueue;


/**
 * The client connection and fail-over handling logic requires some explanation:
 * -when a new client connects, it sends a CONNECT message, with a version number
 * -the server response with CONNECT_OK, and provides its current committed offset
 * -the client sends (SEND) messages with its self-assigned UUID and a monotonically increasing nonce (1-indexed)
 * -the server will respond (ACK) with the nonce, the corresponding global intention offset of this message, and its current
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
 * -the server responds with a sequence of DURABLE messages with the nonce, corresponding global intention offset, and
 *  result state of any committed messages which occur after the given commit offset (sent in-order)
 * -the server then sends a sequence of PENDING message with the nonce, and corresponding global intention offset for any
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
	private static final long MILLIS_BETWEEN_CONNECTION_ATTEMPTS = 100L;

	public static ClientConnection open(InetSocketAddress server) throws IOException {
		if (null == server) {
			throw new IllegalArgumentException("Address cannot be null");
		}
		ClientConnection connection = new ClientConnection(server);
		connection._network.startAndWaitForReady("ClientConnection");
		connection._network.createOutgoingConnection(server);
		return connection;
	}


	/**
	 * The command queue contains all the mutative operations within the ClientConnection instance.  External threads
	 * can add commands to the queue but only the internal thread removes and executes them.
	 * Unless explicitly marked as final, all internal state is considered off-limits for any external thread.  This
	 * means that querying state must also be done on the internal thread, for a consistent answer, resulting in a
	 * somewhat strange pattern where a "container" of a query result is passed into the command and the external caller
	 * waits for it to be populated.
	 * _isClientReady, _mostRecentConnectionFailure, and _currentClusterConfig are the only special cases as they exist
	 * solely to communicate back to the external threads (they are modified and notified under lock).
	 */
	private final UninterruptibleQueue<Void> _commandQueue;
	private InetSocketAddress _serverAddress;
	private InetSocketAddress _redirectAddress;
	private final NetworkManager _network;
	private final UUID _clientId;
	private NetworkManager.NodeToken _connection;
	// We also track the latest config from the cluster - this is currently just used for testing but will eventually be used to govern reconnect decisions.
	private ClusterConfig _currentClusterConfig;
	private Queue<InetSocketAddress> _nextReconnectAttempts;

	private volatile boolean _keepRunning;
	private Thread _internalThread;
	private boolean _canWrite;
	private final List<ClientResult> _outgoingMessages;
	// _inFlightMessages must be sorted since we traverse the keys in-order at the start and end of reconnect.
	// NOTE:  _inFlightMessages can ONLY be accessed by _internalThread.
	private final SortedMap<Long, ClientResult> _inFlightMessages;
	// We keep track of whether or not we have received the CLIENT_READY message so callers can ask if the network is
	// "up" and to ensure we don't write while still waiting for state sync.
	private boolean _isClientReady;
	private long _nextNonce;
	// We store the last global commit the server sends us in responses so we can ask what happened since then, when
	// reconnecting.  It is only read or written by _internalThread.
	private long _lastCommitIntentionOffset;

	// Due to reconnection requirements, it is possible to fail a connection but not want to bring down the system.
	// Therefore, we will continue reconnection attempts, until told to close.  Unless we have an active connection, we
	// store a reference to the most recent failure we observed during connection, for external informational purposes.
	private IOException _mostRecentConnectionFailure;
	// (track whether or not we ever disconnect in order to determine if the next connect is a new connect or reconnect)
	private boolean _hasDisconnectedEver;

	private ClientConnection(InetSocketAddress server) throws IOException {
		_commandQueue = new UninterruptibleQueue<Void>();
		// Make sure we erase the hostname from this, if we were given one (fixes some checks, later on, since the internal system never uses hostnames).
		// This is largely gratuitous but makes some very precise testing possible.
		_serverAddress = new InetSocketAddress(InetAddress.getByAddress(server.getAddress().getAddress()), server.getPort());
		_nextReconnectAttempts = new LinkedList<>();
		_nextReconnectAttempts.add(_serverAddress);
		_network = NetworkManager.outboundOnly(this);
		_clientId = UUID.randomUUID();
		_outgoingMessages = new LinkedList<>();
		_inFlightMessages = new TreeMap<>();
		// First message has nonce of 1L.
		_nextNonce = 1L;
		_keepRunning = true;
		_internalThread = new Thread() {
			@Override
			public void run() {
				_internalThreadMain();
			}
		};
		// Note that it is normally poor form to start the thread in the constructor but this is a private constructor
		// so the matter is 6 vs 1/2 dozen and this is more direct.
		_internalThread.setName("Laminar client (" + _serverAddress + ")");
		_internalThread.start();
	}

	/**
	 * Blocks until the connection to the cluster is successfully established.
	 * This differs from waitForConnectionOrFailure() by ignoring errors which it would through, only returning in case
	 * of a connection or throwing in case of an interruption of the thread by user code.
	 * 
	 * @throws InterruptedException The user interrupted this thread before it had an answer.
	 */
	public synchronized void waitForConnection() throws InterruptedException {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		// Wait until we get a CLIENT_READY message on this connection.
		while (!_isClientReady) {
			this.wait();
		}
	}

	public ClientResult sendCreateTopic(TopicName topic) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot create empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.createTopic(nonce, topic, new byte[0], new byte[0]));
	}

	public ClientResult sendCreateProgrammableTopic(TopicName topic, byte[] code, byte[] arguments) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot create empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.createTopic(nonce, topic, code, arguments));
	}

	public ClientResult sendDestroyTopic(TopicName topic) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot destroy empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.destroyTopic(nonce, topic));
	}

	public ClientResult sendPut(TopicName topic, byte[] key, byte[] value) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot post to empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.put(nonce, topic, key, value));
	}

	public ClientResult sendDelete(TopicName topic, byte[] key) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot post to empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.delete(nonce, topic, key));
	}

	public ClientResult sendPoison(TopicName topic, byte[] key, byte[] value) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot post to empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.poison(nonce, topic, key, value));
	}

	public ClientResult sendStutter(TopicName topic, byte[] key, byte[] value) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		if (topic.string.isEmpty()) {
			throw new IllegalArgumentException("Cannot post to empty topic");
		}
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.stutter(nonce, topic, key, value));
	}

	public ClientResult sendUpdateConfig(ClusterConfig config) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		return _externalWaitForMessageSetup((nonce) -> ClientMessage.updateConfig(nonce, config));
	}

	/**
	 * Every client has a UUID and this is the same after reconnections but new client instances always start with a new
	 * one.
	 * 
	 * @return The UUID of this client.
	 */
	public UUID getClientId() {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		return _clientId;
	}

	/**
	 * @return The nonce that this client will assign to the next message it sends.
	 */
	public long getNextNonce() {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		// We want to get a consistent view of the nonce, which means asking a command to do the query for us.
		long[] container = new long[1];
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				synchronized(container) {
					container[0] = _nextNonce;
					container.notify();
				}
			}});
		boolean interrupt = false;
		synchronized(container) {
			while (0 == container[0]) {
				try {
					container.wait();
				} catch (InterruptedException e) {
					interrupt = true;
				}
			}
		}
		if (interrupt) {
			Thread.currentThread().interrupt();
		}
		return container[0];
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
		Assert.assertTrue(Thread.currentThread() != _internalThread);
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
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		// We can return this without the hand-off since it is really just an asynchronous out-parameter:  putting it in
		// order relative to other operations wouldn't make it more consistent with the server.
		return _currentClusterConfig;
	}

	/**
	 * @return The address of the server the client is interacting with or attempting to interact with (who it thinks is
	 * leading the cluster).
	 */
	public InetSocketAddress getCurrentServer() {
		return _serverAddress;
	}

	/**
	 * This helper is primarily just to stress test reconnect.  It forces the client to reconnect to the server.
	 */
	public void forceReconnect() {
		_commandQueue.put((ignore) -> {
			_network.closeConnection(_connection);
			_internalHandleDisconnect(new IOException("Forced Reconnect"));
		});
	}

	// <INetworkManagerBackgroundCallbacks>
	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		throw Assert.unreachable("Incoming connections not exposed");
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				// Note that it is possible something was still in-flight from when we disconnected so ignore the message, if that is the case.
				if (_connection == node) {
					_canWrite = true;
					_internalTryWrite();
				} else {
					System.err.println("WARNING: Received write-ready on stale connection");
				}
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				// Note that it is possible something was still in-flight from when we disconnected so ignore the message, if that is the case.
				if (_connection == node) {
					_iternalHandleReadableMessage();
				} else {
					System.err.println("WARNING: Received message on stale connection: " + ClientResponse.deserialize(_network.readWaitingMessage(node)).type);
				}
			}});
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				Assert.assertTrue(null == _connection);
				_connection = node;
				// Clear any now-stale connection error.
				_mostRecentConnectionFailure = null;
				// We immediately send our handshake and wait for CLIENT_READY (so we don't set the _canWrite since we claim it).
				// If we have ever been disconnected, this is a reconnect, otherwise the _outgoing messages are just the client jumping the gun.
				ClientMessage handshakeToSend = null;
				if (_hasDisconnectedEver) {
					// Reconnect.
					// We need to send the lowest nonce we don't yet know committed (if any past this did commit, the server will update its state for us when it synthesizes the commits).
					// Note that, if there are no in-flight messages, this is a degenerate reconnect where the server will send us nothing.
					long lowestPossibleNextNonce = _inFlightMessages.isEmpty()
							? (_nextNonce - _outgoingMessages.size())
							: _inFlightMessages.firstKey();
					// Unset any received flags in these since we don't actually know if the other side received them (not in the fail-over case, at least) and rely on the reconnect to fix state.
					for (ClientResult result : _inFlightMessages.values()) {
						result.clearReceived();
					}
					handshakeToSend = ClientMessage.reconnect(lowestPossibleNextNonce, _clientId, _lastCommitIntentionOffset);
				} else {
					// New connection.
					handshakeToSend = ClientMessage.handshake(_clientId);
				}
				_lockedInternalSerializeAndSendMessage(handshakeToSend);
			}});
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				// Note that it is possible something was still in-flight from when we disconnected so ignore the message, if that is the case.
				if (_connection == node) {
					_internalHandleDisconnect(cause);
				} else {
					System.err.println("WARNING: Received disconnect on stale connection");
				}
			}});
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		_commandQueue.putPriority(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				// We just issue the reconnect.
				_internalIssueConnect(cause);
			}}, MILLIS_BETWEEN_CONNECTION_ATTEMPTS);
	}
	// </INetworkManagerBackgroundCallbacks>

	@Override
	public void close() throws IOException {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		// When this command is executed, the internal thread loop will exit and the thread will exit.
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				_keepRunning = false;
			}});
		try {
			_internalThread.join();
		} catch (InterruptedException e) {
			// We don't rely on interruption.
			Assert.unexpected(e);
		}
		_network.stopAndWaitForTermination();
	}


	private void _internalThreadMain() {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		while (_keepRunning) {
			Consumer<Void> command = _commandQueue.blockingGet();
			command.accept(null);
		}
	}

	private boolean _internalReadAndDispatchMessage() {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		byte[] message = _network.readWaitingMessage(_connection);
		// We were told this was here so it can't fail.
		Assert.assertTrue(null != message);
		
		boolean clientBecameReady = false;
		// Decode this.
		ClientResponse deserialized = ClientResponse.deserialize(message);
		// Update the global commit offset.
		// The _lastCommitIntentionOffset generally just stays the same or increases but a decrease can happen after reconnecting to a new leader.
		// We account for this by only increasing the value we have if it is moving in the right direction.
		if (deserialized.lastCommitIntentionOffset > this._lastCommitIntentionOffset) {
			this._lastCommitIntentionOffset = deserialized.lastCommitIntentionOffset;
		}
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
			// from the lastCommitIntentionOffset we set them so they know the nonce of the next message we will send.
			// Now, all we have to do is take any remaining in-flight messages (those not satisfied by the sythesized
			// messages) and push them onto the front of outgoing messages, in the correct nonce order.  Then we can set
			// _isClientReady and the normal logic should play out as though nothing was wrong.
			
			// The CLIENT_READY includes the cluster's active config so we want to read this and verify it is consistent.
			// This just means checking that we are connected to something in it.
			ClusterConfig currentConfig = ((ClientResponsePayload_ClusterConfig)deserialized.payload).config;
			boolean didFind =false;
			for (ConfigEntry entry : currentConfig.entries) {
				didFind = _serverAddress.equals(entry.client);
				if (didFind) {
					break;
				}
			}
			Assert.assertTrue(didFind);
			// Whether this was null (start-up) or something else (if the config change mid-run), we want to set this as our active config in case of a reconnect.
			_currentClusterConfig = currentConfig;
			// Build our queue of reconnect targets.
			_nextReconnectAttempts = new LinkedList<>();
			for (ConfigEntry entry : _currentClusterConfig.entries) {
				_nextReconnectAttempts.add(entry.client);
			}
			
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
		case COMMITTED: {
			CommitInfo info = ((ClientResponsePayload_Commit)deserialized.payload).info;
			result.setCommitted(info);
			// If the message has committed, we will no longer re-send it.
			_inFlightMessages.remove(deserialized.nonce);
		}
			break;
		case UPDATE_CONFIG:
			// This is an out-of-band config update.  This has a -1 nonce so make sure we didn't find a message.
			Assert.assertTrue(null == result);
			// Now, just deserialize and set the config (whether our exising one was null, or not).
			_currentClusterConfig = ((ClientResponsePayload_ClusterConfig)deserialized.payload).config;
			// Build our queue of reconnect targets.
			_nextReconnectAttempts = new LinkedList<>();
			for (ConfigEntry entry : _currentClusterConfig.entries) {
				_nextReconnectAttempts.add(entry.client);
			}
			break;
		case REDIRECT:
			// We need to shut down our current connection and establish a new one to this new cluster leader.
			ConfigEntry newLeader = ((ClientResponsePayload_ConfigEntry)deserialized.payload).entry;
			// We will do this by changing the server address and simulating a dropped connection.
			_redirectAddress = newLeader.client;
			_network.closeConnection(_connection);
			_internalThread.setName("Laminar client (" + _serverAddress + ")");
			_internalHandleDisconnect(new IOException("Redirect received"));
			break;
		default:
			Assert.unreachable("Default response case reached");
			break;
		}
		return clientBecameReady;
	}

	private void _internalSendMessageInWrapper(ClientResult wrapper) {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		// Unwrap the message.
		ClientMessage messageToWrite = wrapper.message;
		_lockedInternalSerializeAndSendMessage(messageToWrite);
		// We also need to track this as an in-flight message.
		_inFlightMessages.put(messageToWrite.nonce, wrapper);
	}

	private void _lockedInternalSerializeAndSendMessage(ClientMessage messageToWrite) {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		// Serialize the message.
		byte[] serialized = messageToWrite.serialize();
		boolean didSend = _network.trySendMessage(_connection, serialized);
		// We only wrote in response to the buffer being empty so this can't fail.
		Assert.assertTrue(didSend);
	}

	private void _internalHandleDisconnect(IOException cause) {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		_connection = null;
		// We will also need to reconnect and wait for CLIENT_READY so the client isn't ready.
		_isClientReady = false;
		// We record that a disconnect happened so our next connection attempt is a reconnect.
		_hasDisconnectedEver = true;
		_commandQueue.putPriority(new Consumer<Void>() {
			@Override
			public void accept(Void t) {
				// We just issue the reconnect.
				_internalIssueConnect(cause);
			}}, MILLIS_BETWEEN_CONNECTION_ATTEMPTS);
	}

	private ClientResult _externalWaitForMessageSetup(Function<Long, ClientMessage> packager) {
		Assert.assertTrue(Thread.currentThread() != _internalThread);
		ClientResult[] container = new ClientResult[1];
		_commandQueue.put(new Consumer<Void>() {
			@Override
			public void accept(Void arg0) {
				ClientMessage message = packager.apply(_nextNonce++);
				ClientResult result = new ClientResult(message);
				_outgoingMessages.add(result);
				synchronized(container) {
					container[0] = result;
					container.notify();
				}
				_internalTryWrite();
			}});
		boolean interrupt = false;
		synchronized(container) {
			while (null == container[0]) {
				try {
					container.wait();
				} catch (InterruptedException e) {
					// We don't want to interrupt this operation but store the flag.
					interrupt = true;
				}
			}
		}
		if (interrupt) {
			Thread.currentThread().interrupt();
		}
		return container[0];
	}

	private void _internalIssueConnect(IOException cause) throws AssertionError {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		// There can be no current connection if there is a connection failure.
		Assert.assertTrue(null == _connection);
		// This happens when a connect or reconnect failure was observed.  Due to reconnection requirements,
		// we just want to try again until this succeeds or we are told to close.
		// We consume the connection failure and move it to _mostRecentConnectionFailure so it is available
		// for the client code's information.
		try {
			// When connecting, we will either first try a redirect address we were given or we will rotate through a list from the config.
			if (null != _redirectAddress) {
				_serverAddress = _redirectAddress;
				_redirectAddress = null;
			} else {
				// Rotate the candidates list.
				_serverAddress = _nextReconnectAttempts.remove();
				_nextReconnectAttempts.add(_serverAddress);
			}
			_network.createOutgoingConnection(_serverAddress);
		} catch (IOException e) {
			// We are just restarting something we already did so a failure here would mean something big changed.
			throw Assert.unexpected(e);
		}
		// Notify anyone who might be waiting to check connection state.
		synchronized(this) {
			_mostRecentConnectionFailure = cause;
			this.notifyAll();
		}
	}

	private void _iternalHandleReadableMessage() {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		// Note that this method may receive a CLIENT_READY message in which case we may need to notify the user thread that the connection is up.
		boolean clientBecameReady = _internalReadAndDispatchMessage();
		if (clientBecameReady) {
			// Notify anyone who might be waiting to check connection state.
			synchronized (this) {
				_isClientReady = true;
				this.notifyAll();
			}
			_internalTryWrite();
		}
	}

	private void _internalTryWrite() {
		Assert.assertTrue(Thread.currentThread() == _internalThread);
		if (!_outgoingMessages.isEmpty() && _canWrite && _isClientReady) {
			// Currently, we only send a single message at a time but this will be made more aggressive in the future.
			ClientResult messageToWrite = _outgoingMessages.remove(0);
			_internalSendMessageInWrapper(messageToWrite);
			_canWrite = false;
		}
	}
}
