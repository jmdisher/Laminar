package com.jeffdisher.laminar.state;

import java.util.UUID;

import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.types.ClientMessagePayload_UpdateConfig;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Miscellaneous helper routines used by NodeState but easier to test and maintain if kept detached from it.
 * These may move elsewhere or be renamed, in the future.
 */
public class Helpers {
	/**
	 * Converts the given ClientMessage from the given clientId into a MutationRecord, assigned the given
	 * mutationOffsetToAssign.
	 * 
	 * @param message The message from the client.
	 * @param termNumber The current term number of the cluster.
	 * @param clientId The client who sent the message.
	 * @param mutationOffsetToAssign The global mutation offset to assign to the created mutation.
	 * @return The MutationRecord for this ClientMessage.
	 */
	public static MutationRecord convertClientMessageToMutation(ClientMessage message, long termNumber, UUID clientId, long mutationOffsetToAssign) {
		MutationRecord converted = null;
		switch (message.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)message.payload).contents;
			converted = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, mutationOffsetToAssign, clientId, message.nonce, contents);
		}
			break;
		case POISON: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)message.payload).contents;
			converted = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, mutationOffsetToAssign, clientId, message.nonce, contents);
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClusterConfig newConfig = ((ClientMessagePayload_UpdateConfig)message.payload).config;
			converted = MutationRecord.generateRecord(MutationRecordType.UPDATE_CONFIG, termNumber, mutationOffsetToAssign, clientId, message.nonce, newConfig.serialize());
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return converted;
	}

	/**
	 * Converts the given mutation into an EventRecord with eventOffsetToAssign as its local event offset.  Note that
	 * this method will return null if the MutationRecord does not convert into an EventRecord.
	 * 
	 * @param mutation The MutationRecord to convert.
	 * @param eventOffsetToAssign The local event offset to assign to the new EventRecord.
	 * @return The corresponding EventRecord or null, if this MutationRecord doesn't convert into an EventRecord.
	 */
	public static EventRecord convertMutationToEvent(MutationRecord mutation, long eventOffsetToAssign) {
		EventRecord eventToReturn;
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case TEMP: {
			eventToReturn = EventRecord.generateRecord(EventRecordType.TEMP, mutation.termNumber, mutation.globalOffset, eventOffsetToAssign, mutation.clientId, mutation.clientNonce, mutation.payload);
		}
			break;
		case UPDATE_CONFIG: {
			// There is no event for UPDATE_CONFIG.
			eventToReturn = null;
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return eventToReturn;
	}
}
