package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;


public interface IClusterManagerCallbacks {
	void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command);

	void mainConnectedToDownstreamPeer(ConfigEntry peer);

	void mainDisconnectedFromDownstreamPeer(ConfigEntry peer);
}
