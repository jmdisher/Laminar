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
		ClientConnection connection = new ClientConnection();
		connection._network.startAndWaitForReady();
		connection._network.createOutgoingConnection(server);
		return connection;
	}


	private final NetworkManager _network;
	private final UUID _clientId;
	private NodeToken _connection;

	private volatile boolean _keepRunning;
	private Thread _backgroundThread;
	private int _pendingMessages;
	private boolean _canWrite;
	private final List<ClientResult> _outgoingMessages;
	// NOTE:  _inFlightMessages can ONLY be accessed by _backgroundThread.
	private final Map<Long, ClientResult> _inFlightMessages;

	private ClientConnection() throws IOException {
		_network = NetworkManager.outboundOnly(this);
		_clientId = UUID.randomUUID();
		_outgoingMessages = new LinkedList<>();
		_inFlightMessages = new HashMap<>();
		_keepRunning = true;
		_backgroundThread = new Thread() {
			@Override
			public void run() {
				_backgroundThreadMain();
			}
		};
		// Note that it is normally poor form to start the thread in the constructor but this is a private constructor
		// so the matter is 6 vs 1/2 dozen and this is more direct.
		_backgroundThread.start();
	}

	public synchronized void waitForConnection() throws InterruptedException {
		while (null == _connection) {
			// Note that we allow interruption since this is client accessible.
			this.wait();
		}
	}

	public synchronized ClientResult sendMessage(ClientMessage message) {
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
		System.out.println("CLIENT " + _clientId + " CONNECTED");
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
				while (_keepRunning && (0 == _pendingMessages) && (_outgoingMessages.isEmpty() || !_canWrite)) {
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
