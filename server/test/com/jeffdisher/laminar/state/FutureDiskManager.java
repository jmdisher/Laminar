package com.jeffdisher.laminar.state;

import org.junit.Assert;

import com.jeffdisher.laminar.disk.IDiskManager;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * An implementation of IDiskManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureDiskManager implements IDiskManager {
	private F<MutationRecord> f_commitMutation;
	private F<EventRecord> f_commitEvent;

	public F<MutationRecord> get_commitMutation() {
		Assert.assertNull(f_commitMutation);
		f_commitMutation = new F<MutationRecord>();
		return f_commitMutation;
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
	public void fetchEvent(long eventToFetch) {
		System.out.println("IDiskManager - fetchEvent");
	}
	@Override
	public void commitMutation(MutationRecord mutation) {
		if (null != f_commitMutation) {
			f_commitMutation.put(mutation);
			f_commitMutation = null;
		} else {
			System.out.println("IDiskManager - commitMutation");
		}
	}
	@Override
	public void commitEvent(EventRecord event) {
		if (null != f_commitEvent) {
			f_commitEvent.put(event);
			f_commitEvent = null;
		} else {
			System.out.println("IDiskManager - commitEvent");
		}
	}
}
