package com.jeffdisher.laminar.state;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jeffdisher.laminar.network.ClientResponse;


/**
 * The information required to track the state of a single connected client.
 */
public class ClientState {
	public final List<ClientResponse> outgoingMessages = new LinkedList<>();
	public boolean writable = true;
	public int readableMessages = 0;
	public UUID clientId = null;
	public long nextNonce = 0;
}
