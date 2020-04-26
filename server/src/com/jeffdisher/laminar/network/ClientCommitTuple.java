package com.jeffdisher.laminar.network;

import java.util.UUID;


/**
 * Instances of this class are used to store data associated with a commit message (to a client) so that the message
 * can be sent once the commit callback comes from the disk layer.
 * The UUID is stored, instead of the NetworkManager.NodeToken because the commit tuple may need to persist over a
 * client's reconnect, meaning the connection token may be different than the same client when the tuple was created.
 */
public class ClientCommitTuple {
	public final UUID clientId;
	public final long clientNonce;
	
	public ClientCommitTuple(UUID clientId, long clientNonce) {
		this.clientId = clientId;
		this.clientNonce = clientNonce;
	}
}
