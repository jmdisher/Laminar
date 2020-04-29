package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_PeerState;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 */
public class ClusterManager implements INetworkManagerBackgroundCallbacks {
	private final Thread _mainThread;
	private final ConfigEntry _self;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;

	// In NodeState, we identify downstream nodes via ClusterConfig.ConfigEntry.
	private final Map<ConfigEntry, NetworkManager.NodeToken> _downstreamNodesByConfig;
	private final Map<NetworkManager.NodeToken, ConfigEntry> _downstreamConfigByNode;

	// Much like ClientManager, we store new upstream peers until we get the handshake from them to know their state.
	private final Set<NetworkManager.NodeToken> _newUpstreamNodes;
	// Once we have the handshake, we move them into ready nodes.
	// (currently, we only know their ConfigEntry so we will just store that).
	private final Map<NetworkManager.NodeToken, ConfigEntry> _readyUpstreamNodes;

	public ClusterManager(ConfigEntry self, ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		_self = self;
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		_downstreamNodesByConfig = new HashMap<>();
		_downstreamConfigByNode = new HashMap<>();
		_newUpstreamNodes = new HashSet<>();
		_readyUpstreamNodes = new HashMap<>();
	}

	public void startAndWaitForReady() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.startAndWaitForReady("ClusterManager");
	}

	public void stopAndWaitForTermination() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.stopAndWaitForTermination();
	}

	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(entry.cluster);
		} catch (IOException e) {
			throw Assert.unimplemented("TODO:  Handle fast-fail on outgoing connections: " + e.getLocalizedMessage());
		}
		_downstreamNodesByConfig.put(entry, token);
		_downstreamConfigByNode.put(token, entry);
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Until we get the cluster handshake from a node, we don't know what to do with it.
				boolean didAdd = _newUpstreamNodes.add(node);
				Assert.assertTrue(didAdd);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Check what kind of node this is, do the appropriate book-keeping, and send any required callback.
				if (_newUpstreamNodes.contains(node)) {
					// We were waiting for a handshake so just drop this.
					_newUpstreamNodes.remove(node);
				} else if (_readyUpstreamNodes.containsKey(node)) {
					ConfigEntry peerConfig = _readyUpstreamNodes.remove(node);
					_callbacks.mainUpstreamPeerDisconnected(peerConfig);
				} else if (_downstreamConfigByNode.containsKey(node)) {
					ConfigEntry peerConfig = _downstreamConfigByNode.remove(node);
					NetworkManager.NodeToken check = _downstreamNodesByConfig.remove(peerConfig);
					Assert.assertTrue(check == node);
					_callbacks.mainDisconnectedFromDownstreamPeer(peerConfig);
				} else {
					// No idea who this is.
					throw Assert.unreachable("Unknown node disconnected");
				}
				// Until we get the cluster handshake from a node, we don't know what to do with it.
				boolean didAdd = _newUpstreamNodes.add(node);
				Assert.assertTrue(didAdd);
			}});
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// Currently, this is ignored since the only writes we send are during the initial handshake.
		// We will just print out what we know about this node.
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				boolean isUpstream = _readyUpstreamNodes.containsKey(node);
				boolean isDownstream = _downstreamConfigByNode.containsKey(node);
				// They can't be write-ready as new and they must be one of these.
				Assert.assertTrue(isUpstream != isDownstream);
				System.out.println("TODO:  Handle nodeWriteReady: " + (isUpstream ? "UPSTREAM" : "DOWNSTREAM"));
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// The handshake will be readable on both sides:
		// -upstream node will send SERVER_IDENTITY
		// -downstream node will respond with PEER_STATE
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Check the relationship with this node.
				if (_downstreamConfigByNode.containsKey(node)) {
					// We are the upstream node so they must be sending the PEER_STATE.
					byte[] payload = _networkManager.readWaitingMessage(node);
					UpstreamResponse response = UpstreamResponse.deserializeFrom(ByteBuffer.wrap(payload));
					Assert.assertTrue(UpstreamResponse.Type.PEER_STATE == response.type);
					// This now counts as us being "connected".
					_callbacks.mainConnectedToDownstreamPeer(_downstreamConfigByNode.get(node), ((UpstreamPayload_PeerState)response.payload).lastReceivedMutationOffset);
				} else {
					// We currently don't allow a "ready" upstream node to send us anything, nor are we handling disconnects at this point, yet.
					Assert.assertTrue(_newUpstreamNodes.contains(node));
					
					// Read the SERVER_IDENTITY.
					byte[] payload = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(payload));
					Assert.assertTrue(DownstreamMessage.Type.IDENTITY == message.type);
					ConfigEntry entry = ((DownstreamPayload_Identity)message.payload).self;
					
					// Send back our PEER_STATE.
					long lastReceivedMutationOffset = 0L;
					UpstreamResponse response = UpstreamResponse.peerState(lastReceivedMutationOffset);
					ByteBuffer buffer = ByteBuffer.allocate(response.serializedSize());
					response.serializeInto(buffer);
					boolean didSend = _networkManager.trySendMessage(node, buffer.array());
					Assert.assertTrue(didSend);
					
					// Migrate this to the ready nodes and notify the callbacks.
					_newUpstreamNodes.remove(node);
					_readyUpstreamNodes.put(node, entry);
					_callbacks.mainUpstreamPeerConnected(entry);
				}
			}});
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Verify that this is still in the map.
				ConfigEntry entry = _downstreamConfigByNode.get(node);
				Assert.assertTrue(null != entry);
				
				// We are the upstream node so send the SERVER_IDENTITY.
				DownstreamMessage identity = DownstreamMessage.identity(_self);
				int size = identity.serializedSize();
				ByteBuffer buffer = ByteBuffer.allocate(size);
				identity.serializeInto(buffer);
				byte[] payload = buffer.array();
				boolean didSend = _networkManager.trySendMessage(node, payload);
				Assert.assertTrue(didSend);
			}});
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// We handle this largely the same way as a connection failure but we also notify the callbacks.
				ConfigEntry entry = _mainRemoveOutboundConnection(node);
				_callbacks.mainDisconnectedFromDownstreamPeer(entry);
			}});
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_mainRemoveOutboundConnection(node);
			}});
	}


	private ConfigEntry _mainRemoveOutboundConnection(NetworkManager.NodeToken node) throws AssertionError {
		// We will unregister this and re-register it with our maps, creating a new connection.
		ConfigEntry entry = _downstreamConfigByNode.remove(node);
		Assert.assertTrue(null != entry);
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(entry.cluster);
		} catch (IOException e) {
			// We previously succeeded in this step so it should still succeed.
			throw Assert.unexpected(e);
		}
		NetworkManager.NodeToken check = _downstreamNodesByConfig.put(entry, token);
		Assert.assertTrue(node == check);
		ConfigEntry checkEntry = _downstreamConfigByNode.put(token, entry);
		Assert.assertTrue(null == checkEntry);
		return entry;
	}
}
