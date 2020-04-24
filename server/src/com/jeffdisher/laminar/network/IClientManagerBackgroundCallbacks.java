package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.ClientState;
import com.jeffdisher.laminar.state.ListenerState;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClientMessage;


/**
 * Callbacks sent by the ClientManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IClientManagerBackgroundCallbacks {
	void ioEnqueueCommandForMainThread(Consumer<StateSnapshot> command);

	void mainNormalClientWriteReady(ClientManager.ClientNode node, ClientState normalState);

	void mainListenerWriteReady(ClientManager.ClientNode node, ListenerState listenerState);

	void mainNormalClientMessageRecieved(ClientManager.ClientNode node, ClientState normalState, ClientMessage incoming);

	void mainRequestMutationFetch(long mutationOffsetToFetch);
}
