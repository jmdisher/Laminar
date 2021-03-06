package com.jeffdisher.laminar.state;

import java.util.List;
import java.util.Set;

import org.junit.Assert;

import com.jeffdisher.laminar.disk.CommittedIntention;
import com.jeffdisher.laminar.disk.IDiskManager;
import com.jeffdisher.laminar.types.CommitInfo.Effect;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * An implementation of IDiskManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureDiskManager implements IDiskManager {
	private F<CommittedIntention> f_commitMutation;
	private F<Consequence> f_commitEvent;

	public F<CommittedIntention> get_commitMutation() {
		F<CommittedIntention> future = new F<>();
		if (null != f_commitMutation) {
			F<CommittedIntention> stem = f_commitMutation;
			while (null != stem.nextLink) {
				stem = stem.nextLink;
			}
			stem.nextLink = future;
		} else {
			f_commitMutation = future;
		}
		return future;
	}

	public F<Consequence> get_commitEvent() {
		Assert.assertNull(f_commitEvent);
		f_commitEvent = new F<Consequence>();
		return f_commitEvent;
	}

	@Override
	public void restoreState(Set<TopicName> activeTopics) {
		System.out.println("IDiskManager - restoreState");
	}

	@Override
	public void fetchIntention(long mutationOffset) {
		System.out.println("IDiskManager - fetchMutation");
	}
	@Override
	public void fetchConsequence(TopicName topic, long eventToFetch) {
		System.out.println("IDiskManager - fetchEvent");
	}
	@Override
	public void commit(Intention intention, Effect effect, List<Consequence> consequences, byte[] newTransformedCode, byte[] objectGraph) {
		if (null != f_commitMutation) {
			f_commitMutation.put(CommittedIntention.create(intention, effect));
			f_commitMutation = f_commitMutation.nextLink;
		} else {
			System.out.println("IDiskManager - commit(intention)");
		}
		if (null != consequences) {
			for (Consequence consequence : consequences) {
				if (null != f_commitEvent) {
					f_commitEvent.put(consequence);
					f_commitEvent = f_commitEvent.nextLink;
				} else {
					System.out.println("IDiskManager - commit(consequence)");
				}
			}
		}
	}
}
