package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;


/**
 * Instances of this class are used to store data associated with a commit message (to a client) so that the message
 * can be sent once the commit callback comes from the disk layer.
 */
public class ClientCommitTuple {
	public final NetworkManager.NodeToken client;
	public final long clientNonce;
	
	public ClientCommitTuple(NetworkManager.NodeToken client, long clientNonce) {
		this.client = client;
		this.clientNonce = clientNonce;
	}
}
