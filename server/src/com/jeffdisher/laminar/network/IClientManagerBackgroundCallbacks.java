package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.state.ClientState;
import com.jeffdisher.laminar.state.ListenerState;


/**
 * Callbacks sent by the ClientManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IClientManagerBackgroundCallbacks {
	void ioEnqueueCommandForMainThread(Runnable command);

	void mainNormalClientWriteReady(ClientManager.ClientNode node, ClientState normalState);

	void mainListenerWriteReady(ClientManager.ClientNode node, ListenerState listenerState);

	void clientReadReady(ClientManager.ClientNode node);
}
