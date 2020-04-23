package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.network.ClientManager.ClientNode;
import com.jeffdisher.laminar.types.ClientResponse;


/**
 * Instances of this class are used to store data associated with a commit message (to a client) so that the message
 * can be sent once the commit callback comes from the disk layer.
 * The "specialAction" is typically null but some actions (like UPDATE_CONFIG) want to do something special when
 * they commit.
 */
public class ClientCommitTuple {
	public final ClientNode client;
	public final ClientResponse ack;
	public final Runnable specialAction;
	
	public ClientCommitTuple(ClientNode client, ClientResponse ack, Runnable specialAction) {
		this.client = client;
		this.ack = ack;
		this.specialAction = specialAction;
	}
}
