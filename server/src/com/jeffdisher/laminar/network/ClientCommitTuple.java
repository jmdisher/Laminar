package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.network.ClientManager.ClientNode;


/**
 * Instances of this class are used to store data associated with a commit message (to a client) so that the message
 * can be sent once the commit callback comes from the disk layer.
 */
public class ClientCommitTuple {
	public final ClientNode client;
	public final long clientNonce;
	
	public ClientCommitTuple(ClientNode client, long clientNonce) {
		this.client = client;
		this.clientNonce = clientNonce;
	}
}
