package com.jeffdisher.laminar.state;

import org.junit.Assert;

import com.jeffdisher.laminar.disk.CommittedMutationRecord;
import com.jeffdisher.laminar.disk.IDiskManager;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;


/**
 * An implementation of IDiskManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureDiskManager implements IDiskManager {
	private F<CommittedMutationRecord> f_commitMutation;
	private F<EventRecord> f_commitEvent;

	public F<CommittedMutationRecord> get_commitMutation() {
		F<CommittedMutationRecord> future = new F<>();
		if (null != f_commitMutation) {
			F<CommittedMutationRecord> stem = f_commitMutation;
			while (null != stem.nextLink) {
				stem = stem.nextLink;
			}
			stem.nextLink = future;
		} else {
			f_commitMutation = future;
		}
		return future;
	}

	public F<EventRecord> get_commitEvent() {
		Assert.assertNull(f_commitEvent);
		f_commitEvent = new F<EventRecord>();
		return f_commitEvent;
	}

	@Override
	public void fetchMutation(long mutationOffset) {
		System.out.println("IDiskManager - fetchMutation");
	}
	@Override
	public void fetchEvent(TopicName topic, long eventToFetch) {
		System.out.println("IDiskManager - fetchEvent");
	}
	@Override
	public void commitMutation(CommittedMutationRecord mutation) {
		if (null != f_commitMutation) {
			f_commitMutation.put(mutation);
			f_commitMutation = f_commitMutation.nextLink;
		} else {
			System.out.println("IDiskManager - commitMutation");
		}
	}
	@Override
	public void commitEvent(TopicName topic, EventRecord event) {
		if (null != f_commitEvent) {
			f_commitEvent.put(event);
			f_commitEvent = f_commitEvent.nextLink;
		} else {
			System.out.println("IDiskManager - commitEvent");
		}
	}
}
