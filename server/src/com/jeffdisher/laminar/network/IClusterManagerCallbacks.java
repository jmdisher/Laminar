package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;


public interface IClusterManagerCallbacks {
	void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command);

	void mainConnectedToDownstreamPeer(ClusterConfig.ConfigEntry peer);

	void mainDisconnectedFromDownstreamPeer(ClusterConfig.ConfigEntry peer);
}
