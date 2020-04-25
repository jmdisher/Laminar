package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;


public interface IClusterManagerCallbacks {
	void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command);
}
