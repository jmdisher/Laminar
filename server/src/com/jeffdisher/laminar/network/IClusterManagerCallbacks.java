package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;


public interface IClusterManagerCallbacks {
	void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command);

	void mainConnectedToDownstreamPeer(ConfigEntry peer, long lastReceivedMutationOffset);

	void mainDisconnectedFromDownstreamPeer(ConfigEntry peer);

	void mainUpstreamPeerConnected(ConfigEntry peer);

	void mainUpstreamPeerDisconnected(ConfigEntry peer);

	void mainDownstreamPeerWriteReady(ConfigEntry peer);

	void mainDownstreamPeerReceivedMutations(ConfigEntry peer, long lastReceivedMutationOffset);

	void mainUpstreamPeerWriteReady(ConfigEntry peer);

	void mainUpstreamSentMutation(ConfigEntry peer, MutationRecord record, long lastCommittedMutationOffset);
}
