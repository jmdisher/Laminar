package com.jeffdisher.laminar.network;


/**
 * Callbacks sent by the ClientManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IClientManagerBackgroundCallbacks {
	void clientConnectedToUs(ClientManager.ClientNode node);

	void clientDisconnectedFromUs(ClientManager.ClientNode node);

	void clientWriteReady(ClientManager.ClientNode node);

	void clientReadReady(ClientManager.ClientNode node);
}
