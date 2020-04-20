package com.jeffdisher.laminar.network;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Common abstraction over a logical network use-case.  While it is possible to have all network use-cases share the
 * same manager, we will keep them distinct for the time being.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 * Note on message framing:  All network messages, including the header, are limited to 64 KiB.  The header is 2-bytes
 * (the big-endian u16 of the size) so this means the usable payload can be between [0..65534].
 * 
 * Concerns regarding concurrent access to NIO resources:
 * 1)  Registering a channel with a selector seems to block on any other interaction with the selector, mostly notably
 *  a blocking select operation.  For the most part, these registrations are only done on the internal but outgoing
 *  connections are created/destroyed on an external caller.  For this reasons, we will use a hand-off so that the
 *  internal thread wakes from the select operation to do the registration on behalf of the caller.
 * 2)  Changing the interested operations of an existing registration _appears_ to be safe and not blocked by an ongoing
 *  select operation so we will still change those on the outside.  This may be unreliable and the documentation states
 *  that this is implementation-dependent so this may chance in the future.
 * 
 * Locking strategy:
 * -we always lock on the entire NetworkManager's monitor even though finer-grained is possible (individual
 *  ConnectionState).
 * -this is done just to simplify the mental model around this.  Nothing expensive or blocking is done in these locked
 *  sections so the lock splitting would be an unnecessary cost.
 * -all instance variables are only touched by the internal/background thread, with the exception of "_handoff_*"
 *  variables and the ConnectionState objects resolved by public interface calls (corresponding NodeToken and
 *  SelectorKey are immutable relationships).
 * -no lock is held over a call to callback target.
 */
public class NetworkManager {
	// We will use 64 KiB buffers since we will impose a message size limit less than this.
	private static final int BUFFER_SIZE_BYTES = 64 * 1024;
	// We reserve 2 bytes in the message payload for the big-endian u16 size so the maximum payload is 65534 bytes.
	public static final int MESSAGE_PAYLOAD_MAXIMUM_BYTES = 64 * 1024 - 2;

	/**
	 * Creates a NetworkManager which is set to receive incoming connections on serverSocket and can also be used to
	 * make outgoing connections.
	 * 
	 * @param serverSocket The socket where incoming connections will be received.
	 * @param callbackTarget Target for background thread callbacks.
	 * @return A NetworkManager configured to allow incoming and outgoing connections.
	 * @throws IOException Something went wrong when setting up the network infrastructure.
	 */
	public static NetworkManager bidirectional(ServerSocketChannel serverSocket, INetworkManagerBackgroundCallbacks callbackTarget) throws IOException {
		if (null == serverSocket) {
			throw new IllegalArgumentException("Server socket must be provided");
		}
		return new NetworkManager(serverSocket, callbackTarget);
	}

	/**
	 * Creates a NetworkManager which is set to allow only outgoing connections to be created.
	 * 
	 * @param callbackTarget Target for background thread callbacks.
	 * @return A NetworkManager configured to allow outgoing connections only.
	 * @throws IOException Something went wrong when setting up the network infrastructure.
	 */
	public static NetworkManager outboundOnly(INetworkManagerBackgroundCallbacks callbackTarget) throws IOException {
		return new NetworkManager(null, callbackTarget);
	}


	private final Selector _selector;
	private final ServerSocketChannel _acceptorSocket;
	private final SelectionKey _acceptorKey;
	// Note that the _connectedNodes contains all connected sockets, incoming or outgoing.
	private final LinkedList<SelectionKey> _connectedNodes;
	private final INetworkManagerBackgroundCallbacks _callbackTarget;

	// Hand-offs used for opening/closing out-going connections.
	private volatile SocketChannel _handoff_newConnection;
	private volatile NodeToken _handoff_newConnectionResponse;
	private volatile NodeToken _handoff_closeConnection;

	// We will mark this volatile since we are relying on the select, not the monitor.
	private volatile boolean _keepRunning;
	private Thread _background;

	private NetworkManager(ServerSocketChannel clusterSocket, INetworkManagerBackgroundCallbacks callbackTarget) throws IOException {
		// This can throw IOException which always feels odd in a constructor so maybe this should be a static method.
		// (could be passed in but this seems like an internal concern)
		_selector = Selector.open();
		if (null != clusterSocket) {
			// Configure the cluster server socket for use with the selector.
			clusterSocket.configureBlocking(false);
			// If this isn't a server socket, this is a static error.
			int serverSocketOps = clusterSocket.validOps();
			Assert.assertTrue(SelectionKey.OP_ACCEPT == serverSocketOps);
			_acceptorSocket = clusterSocket;
			// Note that we normally put ConnectionState in attachment, but there isn't one for the server socket.
			_acceptorKey = clusterSocket.register(_selector, serverSocketOps, null);
		} else {
			_acceptorSocket = null;
			_acceptorKey = null;
		}
		// We put the connected nodes in a LinkedList since we want a dense list, we rarely change it, and often need to walk, in-order.
		_connectedNodes = new LinkedList<>();
		_callbackTarget = callbackTarget;
	}

	public void startAndWaitForReady(String consumer) {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		// (this is gratuitous here but is added for consistency)
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (_keepRunning) {
			throw new IllegalStateException("Background thread already running");
		}
		if (null != _background) {
			throw new IllegalStateException("NetworkManager is not restartable");
		}
		_keepRunning = true;
		_background = new Thread() {
			@Override
			public void run() {
				_backgroundThreadMain();
			}
		};
		_background.setName("Laminar network - " + consumer);
		_background.start();
	}

	public void stopAndWaitForTermination() {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (!_keepRunning) {
			throw new IllegalStateException("Background thread not running");
		}
		// We can use the wakeup() method, instead of needing a pipe to break the select and a monitor to notify.
		_keepRunning = false;
		_selector.wakeup();
		// Wait for shutdown.
		try {
			_background.join();
		} catch (InterruptedException e) {
			// We don't use interruption.
			Assert.unexpected(e);
		}
		if (null != _acceptorKey) {
			// Since we registered the initial selector, we need to cancel it.
			_acceptorKey.cancel();
		}
		// We also expect the selector's key set to now be empty (we need to force it to update this).
		try {
			_selector.selectNow();
		} catch (IOException e) {
			// We are only doing this for purposes of updating the keys for the assert so we will ignore this.
		}
		Assert.assertTrue(_selector.keys().isEmpty());
	}

	/**
	 * Attempts to write the given message payload to the outgoing buffer, failing if the buffer can't fit the payload.
	 * Note that, if this returns false, the caller should wait for nodeWriteReady.
	 * In the case of an asynchronous disconnect, this method may still return true even if it knows that the message
	 * will never be written to the network.  This to hide the inherent race in network interactions from the caller.
	 * They will get the same answer they _could_ have received if the call happened a few microseconds earlier.
	 * 
	 * @param target The node where the message should be sent.
	 * @param payload The message payload to send.
	 * @return True if the payload was added to the write buffer, false if it couldn't fit.
	 * @throws IllegalArgumentException If the payload is larger than MESSAGE_PAYLOAD_MAXIMUM_BYTES.
	 */
	public boolean trySendMessage(NetworkManager.NodeToken target, byte[] payload) throws IllegalArgumentException {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (!_keepRunning) {
			throw new IllegalStateException("Background thread not running");
		}
		if (payload.length > MESSAGE_PAYLOAD_MAXIMUM_BYTES) {
			throw new IllegalArgumentException("Buffer size greater than " + MESSAGE_PAYLOAD_MAXIMUM_BYTES);
		}
		
		boolean didSend = false;
		ConnectionState state = (ConnectionState) target.actualKey.attachment();
		synchronized (this) {
			// See if there is enough space in the buffer for this message.
			int spaceRequired = payload.length + Short.BYTES;
			// The check above made sure that this size can fit, but it may show up as negative on the Java side.
			short size = (short) payload.length;
			ByteBuffer buffer = state.toWrite;
			if (buffer.remaining() >= spaceRequired) {
				boolean willNeedWriteInterest = (0 == buffer.position());
				buffer.putShort(size);
				buffer.put(payload);
				
				// Note that we only want to enable writing if we weren't already in a writing state and if the connection hasn't closed.
				if (willNeedWriteInterest && !state.isClosed) {
					// If the buffer _was_ empty, we know we now need to change the interested ops.
					// Note that it is safe to write this thread, even though the background is consuming it (according
					// to docs).
					// We are doing this under state lock to avoid racing problems where this write may finish before we
					// update it (we should never attempt to write from a buffer which is empty).
					target.actualKey.interestOps(target.actualKey.interestOps() | SelectionKey.OP_WRITE);
					_selector.wakeup();
				}
				didSend = true;
			}
		}
		return didSend;
	}

	/**
	 * Reads a message payload from the incoming buffer for the given sender.
	 * If there is no complete message, null is returned.
	 * 
	 * @param sender The node from which the message was sent.
	 * @return The message payload or null if a complete message wasn't available.
	 */
	public byte[] readWaitingMessage(NetworkManager.NodeToken sender) {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (!_keepRunning) {
			throw new IllegalStateException("Background thread not running");
		}
		byte[] message = null;
		ConnectionState state = (ConnectionState) sender.actualKey.attachment();
		synchronized (this) {
			// Read the size.
			ByteBuffer buffer = state.toRead;
			if (buffer.position() >= Short.BYTES) {
				boolean willNeedReadInterest = (0 == buffer.remaining());
				buffer.flip();
				int size = Short.toUnsignedInt(buffer.getShort());
				if (buffer.remaining() >= size) {
					// We have enough data so read it and compact the buffer.
					message = new byte[size];
					buffer.get(message);
					buffer.compact();
					
					// Note that we only want to enable reading if we weren't already in a reading state and if the connection hasn't closed.
					if (willNeedReadInterest && !state.isClosed) {
						// If the buffer was full, we now need to re-add the reading interest.
						// Note that it is safe to write this thread, even though the background is consuming it
						// (according to docs).
						// We are doing this under state lock to avoid racing problems where this buffer may fill before
						// we update it (we should never attempt to read into a buffer which is full).
						sender.actualKey.interestOps(sender.actualKey.interestOps() | SelectionKey.OP_READ);
						_selector.wakeup();
					}
				} else {
					// We can't do the read so rewind the position and "unflip".
					buffer.position(buffer.limit());
					buffer.limit(buffer.capacity());
				}
			}
		}
		return message;
	}

	public NodeToken createOutgoingConnection(InetSocketAddress address) throws IOException {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (!_keepRunning) {
			throw new IllegalStateException("Background thread not running");
		}
		// Create the socket as non-blocking and initiate the connection.
		// This is then added to our _persistentOutboundNodes set, since we want to re-establish these if they drop.
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(false);
		boolean isImmediatelyConnected = outbound.connect(address);
		// TODO: Remove this once we can test if immediate connections happen and what the implications are, if it does.
		if (isImmediatelyConnected) {
			System.out.println("IMMEDIATE CONNECT");
		}
		// Note:  This part seems gratuitously complex but we want the _internal_ thread to register the connection and
		// interact with _connectedNodes, ONLY.
		// This allows enhanced safety around _connectedNodes but mostly it is to avoid an issue where registering with
		// the selector will block if an active select is in-progress.
		// Also, since creating/destroying connections is a rare an expensive operation, this should be safe.
		NodeToken token = null;
		synchronized (this) {
			// Make sure we aren't queued behind someone else.
			while (null != _handoff_newConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			_handoff_newConnection = outbound;
			// Notify the internal thread (it isn't blocked in a monitor, but in the select).
			_selector.wakeup();
			while (null == _handoff_newConnectionResponse) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			token = _handoff_newConnectionResponse;
			_handoff_newConnectionResponse = null;
		}
		return token;
	}

	/**
	 * Request that a given connection, be it incoming or outgoing, be forcibly closed.
	 * If the connection is already closed, this has no effect (as it is possible for an error to asynchronously
	 * initiate a close in a way which would race with these explicit attempts), and sends no disconnected callbacks.
	 * 
	 * @param token The connection to close.
	 */
	public void closeConnection(NodeToken token) {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		if (!_keepRunning) {
			throw new IllegalStateException("Background thread not running");
		}
		SocketChannel channel = ((ConnectionState)(token.actualKey).attachment()).channel;
		// The _internal_ thread owns outgoing connection open/close so hand this off to them.
		synchronized (this) {
			// Make sure we aren't queued behind someone else.
			while (null != _handoff_closeConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			_handoff_closeConnection = token;
			// Notify the internal thread (it isn't blocked in a monitor, but in the select).
			_selector.wakeup();
			while (null != _handoff_closeConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
		}
		// By this point, we can proceed to close the channel (symmetry with the opening of the channel).
		try {
			channel.close();
		} catch (IOException e) {
			// If there is an exception on close, we just abandon the socket.
			// It isn't clear what causes of this exist which aren't equivalent to it already being closed.
		}
	}


	private void _backgroundThreadMain() {
		while (_keepRunning) {
			int selectedKeyCount = 0;
			try {
				selectedKeyCount = _selector.select();
			} catch (IOException e) {
				// TODO:  Determine how we want to handle this once we observe what kind of IO error can happen here.
				e.printStackTrace();
				throw Assert.unimplemented(e.getLocalizedMessage());
			}
			// We are often just woken up to update the set of interested operations so this may be empty.
			if (selectedKeyCount > 0) {
				_backgroundProcessSelectedKeys();
			}
			// Note that the keys' interested operations are updated by processing why they were selected or when an
			// external caller changes the state so we don't need to go through them, again.
			
			// Also, we may have an outgoing connection pending so check that.
			_backgroundCheckHandoff();
		}
		// We are shutting down so close all clients.
		for (SelectionKey key : _connectedNodes) {
			ConnectionState state = (ConnectionState)key.attachment();
			try {
				state.channel.close();
			} catch (IOException e) {
				// This is a shutdown so just drop the exception and proceed.
			}
			key.cancel();
		}
	}

	private synchronized void _backgroundCheckHandoff() {
		if (null != _handoff_newConnection) {
			// Register this connection, pass back the token, and notify them.
			ConnectionState newState = new ConnectionState(_handoff_newConnection, true);
			SelectionKey newKey;
			try {
				newKey = _handoff_newConnection.register(_selector, SelectionKey.OP_CONNECT, newState);
			} catch (ClosedChannelException e) {
				// Due to the way we manage our channels, we will never close one while still interacting it elsewhere so this would be a static violation of that rule.
				throw Assert.unexpected(e);
			}
			NodeToken token = new NodeToken(newKey);
			newState.token = token;
			_connectedNodes.add(newKey);
			
			_handoff_newConnection = null;
			// The last caller would have consumed the response before the new caller could have passed us work.
			Assert.assertTrue(null == _handoff_newConnectionResponse);
			_handoff_newConnectionResponse = token;
			
			this.notifyAll();
		}
		
		// Check the disconnect, as well.
		if (null != _handoff_closeConnection) {
			SelectionKey key = _handoff_closeConnection.actualKey;
			_connectedNodes.remove(key);
			key.cancel();
			// Since we are cancelling the key here, we also need to set the closed flag on the corresponding state.
			((ConnectionState)key.attachment()).isClosed = true;
			// (note that we only cancel the registration - closing the socket is done in the calling thread).
			
			_handoff_closeConnection = null;
			this.notifyAll();
		}
	}

	private void _backgroundProcessSelectedKeys() {
		Iterator<SelectionKey> selectedKeys = _selector.selectedKeys().iterator();
		while (selectedKeys.hasNext()) {
			SelectionKey key = selectedKeys.next();
			// The key "isValid" will only be set false by our attempts to cancel on disconnect, below in this method, but it should start out valid.
			Assert.assertTrue(key.isValid());
			if (key == _acceptorKey) {
				_backgroundProcessAcceptorKey(key);
			} else {
				// This is normal data movement so get the state out of the attachment.
				ConnectionState state = (ConnectionState)key.attachment();
				// We can't fail to find this since we put it in the collection.
				Assert.assertTrue(null != state);
				
				// See what operation we wanted to perform.
				if (key.isConnectable()) {
					_backgroundProcessConnectableKey(key, state);
				}
				if (key.isValid() && key.isReadable()) {
					_backgroundProcessReadableKey(key, state);
				}
				if (key.isValid() && key.isWritable()) {
					_backgroundProcessWritableKey(key, state);
				}
			}
			selectedKeys.remove();
		}
	}

	private void _backgroundProcessAcceptorKey(SelectionKey key) {
		// This must be a new node connecting.
		Assert.assertTrue(SelectionKey.OP_ACCEPT == key.readyOps());
		// This cannot be null if we matched the acceptor key.
		Assert.assertTrue(null != _acceptorSocket);
		SocketChannel newNode;
		try {
			newNode = _acceptorSocket.accept();
		} catch (IOException e) {
			// We don't know what problem would result in an IOException during accept so flag this as a bug.
			throw Assert.unexpected(e);
		}
		
		// Configure this new node for our selection set - by default, it starts only waiting for read.
		try {
			newNode.configureBlocking(false);
		} catch (IOException e) {
			// Changing this state shouldn't involve an IOException so flag that as fatal, if it happens.
			throw Assert.unexpected(e);
		}
		ConnectionState newState = new ConnectionState(newNode, false);
		SelectionKey newKey;
		try {
			newKey = newNode.register(_selector, SelectionKey.OP_READ, newState);
		} catch (ClosedChannelException e) {
			// We just created this channel so this can't happen.
			throw Assert.unexpected(e);
		}
		NodeToken token = new NodeToken(newKey);
		newState.token = token;
		_connectedNodes.add(newKey);
		// Notify the callbacks.
		_callbackTarget.nodeDidConnect(token);
	}

	private void _backgroundProcessConnectableKey(SelectionKey key, ConnectionState state) {
		// Finish the connection, send the callback that an outbound connection was established, and switch
		// its interested ops (normally just READ but WRITE is possible if data has already been written to
		// the outgoing buffer.
		boolean isConnected;
		try {
			isConnected = state.channel.finishConnect();
			// This would only return false if we hadn't selected on connectability.
			Assert.assertTrue(isConnected);
		} catch (IOException e) {
			// This is typically "Connection refused".
			// We just want to close the connection, cancel the key, and send the callback.
			// Since this connection was still finishing, we haven't yet told them anything about this.  We still want
			// to flag the connection "isClosed", just for consistency, and notify them that the connection failed.
			try {
				state.channel.close();
			} catch (IOException e1) {
				// This is just book-keeping so it can't fail.
				Assert.unexpected(e1);
			}
			key.cancel();
			state.isClosed = true;
			_connectedNodes.remove(key);
			_callbackTarget.outboundNodeConnectionFailed(state.token, e);
			isConnected = false;
		}
		if (isConnected) {
			int interestedOps = SelectionKey.OP_READ;
			synchronized (this) {
				if (state.toWrite.position() > 0) {
					interestedOps |= SelectionKey.OP_WRITE;
				}
			}
			key.interestOps(interestedOps);
			_callbackTarget.outboundNodeConnected(state.token);
		}
	}

	private void _backgroundProcessReadableKey(SelectionKey key, ConnectionState state) {
		// Read into our buffer.
		int newMessagesAvailable = 0;
		// Note that we may need to send a callback if we detect a disconnect here but we can't do that under lock.
		NodeToken outboundNodeDisconnected = null;
		NodeToken inboundNodeDisconnected = null;
		IOException disconnectException = null;
		synchronized (this) {
			int originalPosition = state.toRead.position();
			try {
				int read = state.channel.read(state.toRead);
				if (-1 == read) {
					// If the remote side closes the connection, this typically returns a -1 as an EOF.
					disconnectException = _synthesizeEndOfFile();
				}
			} catch (IOException e) {
				// This is typically a "Connection reset by peer".
				disconnectException = e;
			}
			
			if (null == disconnectException) {
				if (0 == state.toRead.remaining()) {
					// If this buffer is now full, stop reading.
					key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
				}
				
				// Determine if this read operation added a new completed message to the buffer.
				ByteBuffer readOnly = state.toRead.asReadOnlyBuffer();
				readOnly.flip();
				boolean keepReading = true;
				while (keepReading && ((readOnly.position() + Short.BYTES) <= readOnly.limit())) {
					int size = Short.toUnsignedInt(readOnly.getShort());
					if ((readOnly.position() + size) <= readOnly.limit()) {
						readOnly.position(readOnly.position() + size);
						// If we are now pointing after the original position, this must be a new message.
						if (readOnly.position() > originalPosition) {
							newMessagesAvailable += 1;
						}
					} else {
						keepReading = false;
					}
				}
			} else {
				// We just want to close the connection, cancel the key, and send the callback.
				// NOTE:  The buffer may already have fully-loaded message frames for which we already notified the
				// callback target so we will need to close the port, cancel the key, remove this from our list of open
				// connections, and set the state to be closed so that we won't try to re-enable it when they consume
				// the message.
				try {
					state.channel.close();
				} catch (IOException e1) {
					// This is just book-keeping so it can't fail.
					Assert.unexpected(e1);
				}
				key.cancel();
				_connectedNodes.remove(key);
				state.isClosed = true;
				if (state.isOutgoing) {
					outboundNodeDisconnected = state.token;
				} else {
					inboundNodeDisconnected = state.token;
				}
			}
		}
		// Notify the callbacks of each new message.
		for (int i = 0; i < newMessagesAvailable; ++i) {
			// Just to prove that we are doing this correctly and didn't re-order something, above, make sure this is still open.
			Assert.assertTrue(!state.isClosed);
			_callbackTarget.nodeReadReady(state.token);
		}
		// Send the disconnect callbacks.
		if (null != outboundNodeDisconnected) {
			_callbackTarget.outboundNodeDisconnected(outboundNodeDisconnected, disconnectException);
		}
		if (null != inboundNodeDisconnected) {
			_callbackTarget.nodeDidDisconnect(inboundNodeDisconnected, disconnectException);
		}
	}

	private void _backgroundProcessWritableKey(SelectionKey key, ConnectionState state) {
		// Write from our buffer.
		boolean isBufferEmpty = false;
		// Note that we may need to send a callback if we detect a disconnect here but we can't do that under lock.
		NodeToken outboundNodeDisconnected = null;
		NodeToken inboundNodeDisconnected = null;
		IOException disconnectException = null;
		synchronized (this) {
			state.toWrite.flip();
			try {
				int written = state.channel.write(state.toWrite);
				// If this returns no writes, it means this is in a state we can't interpret.
				Assert.assertTrue(written > 0);
			} catch (IOException e) {
				// This is typically a "Broken pipe".
				disconnectException = e;
				// We just want to close the connection, cancel the key, and send the callback.
				// Note that we may have already told the caller that we were write-ready and they may, in fact, already
				// be in the middle of a call to write a message, blocked on this monitor.  We will just record that the
				// channel is closed so their interaction won't try to re-enable the write selector.
				try {
					state.channel.close();
				} catch (IOException e1) {
					// This is just book-keeping so it can't fail.
					Assert.unexpected(e1);
				}
				key.cancel();
				_connectedNodes.remove(key);
				state.isClosed = true;
				if (state.isOutgoing) {
					outboundNodeDisconnected = state.token;
				} else {
					inboundNodeDisconnected = state.token;
				}
			}
			
			if (null == disconnectException) {
				state.toWrite.compact();
				isBufferEmpty = 0 == state.toWrite.position();
				if (isBufferEmpty) {
					// If this buffer is now empty, stop writing.
					key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
				}
			}
		}
		// Now that we are out of lock, notify the callback target.
		if (isBufferEmpty) {
			// Just to prove that we are doing this correctly and didn't re-order something, above, make sure this is still open.
			Assert.assertTrue(!state.isClosed);
			// We need to notify the callbacks that the buffer has fully drained.
			_callbackTarget.nodeWriteReady(state.token);
		}
		// Send the disconnect callbacks.
		if (null != outboundNodeDisconnected) {
			_callbackTarget.outboundNodeDisconnected(outboundNodeDisconnected, disconnectException);
		}
		if (null != inboundNodeDisconnected) {
			_callbackTarget.nodeDidDisconnect(inboundNodeDisconnected, disconnectException);
		}
	}

	private IOException _synthesizeEndOfFile() {
		// We use a helper method to create these since we might want to do something different but this seems appropriate.
		return new EOFException("Synthetic EOF");
	}


	/**
	 * The internal representation of the buffer state around a connection.
	 * Note that all access to the buffers in this state MUST be done under the state's monitor.
	 */
	private static class ConnectionState {
		// Note that a channel is available directly from the SelectionKey but it is the wrong type so this is here to
		// avoid a down-cast (might switch to that later on but this keeps it clear).
		public final SocketChannel channel;
		public final boolean isOutgoing;  //False will be incoming.
		public final ByteBuffer toRead = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
		public final ByteBuffer toWrite = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
		// Token is written after the key is created.
		public NodeToken token;
		// We set this closed flag if the connection is closed and can still be read but cannot be enqueued for more reading.
		public boolean isClosed;
		
		public ConnectionState(SocketChannel channel, boolean isOutgoing) {
			this.channel = channel;
			this.isOutgoing = isOutgoing;
		}
	}


	/**
	 * This may seem like over-kill but this opaque wrapper of the SelectionKey is what we will use to represent a
	 * connection, outside of this class.
	 */
	public static class NodeToken {
		private final SelectionKey actualKey;
		private NodeToken(SelectionKey actualKey) {
			this.actualKey = actualKey;
		}
	}
}
