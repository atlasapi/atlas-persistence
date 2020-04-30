package org.atlasapi.messaging.v3;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EquivalenceChangeMessengerTest {
    private final MessageSender<EquivalenceChangeMessage> sender = mock(MessageSender.class);
    private final Timestamper timestamper = mock(Timestamper.class);
    private EquivalenceChangeMessenger equivalenceChangeMessenger;

    @Before
    public void setUp() throws Exception {
        equivalenceChangeMessenger = EquivalenceChangeMessenger.create(
                sender,
                timestamper
        );
    }

    @Test
    public void testCorrectChangesAreDeterminedFromLookupEntries() throws Exception {

        Item item1 = item(1);
        LookupRef ref2 = ref(2);
        LookupRef ref3 = ref(3);
        LookupRef ref4 = ref(4);

        LookupEntry original = LookupEntry.lookupEntryFrom(item1);
        original = original.copyWithDirectEquivalents(
                original.directEquivalents()
                        .copyWithLinks(ImmutableSet.of(ref2, ref3), EquivRefs.Direction.OUTGOING)
        );

        LookupEntry updated = original.copyWithDirectEquivalents(
                original.directEquivalents()
                        .copyWithoutLink(ref3, EquivRefs.Direction.OUTGOING)
                        .copyWithLink(ref4, EquivRefs.Direction.OUTGOING)
        );

        Set<String> sources = ImmutableSet.of(Publisher.TESTING_MBST.key());

        when(timestamper.timestamp()).thenReturn(Timestamp.of(1L));

        equivalenceChangeMessenger.sendMessageFromDirectEquivs(original, updated, sources);

        verify(sender, times(1)).sendMessage(
                argThat(messageMatcher(
                        item1.getId(),
                        ImmutableSet.of(ref4.id()),
                        ImmutableSet.of(ref3.id()),
                        ImmutableSet.of(item1.getId(), ref2.id()),
                        sources
                )),
                any()
        );
    }

    private Item item(long id) {
        Item item = new Item();
        item.setCanonicalUri("uri" + id);
        item.setId(id);
        item.setPublisher(Publisher.TESTING_MBST);
        return item;
    }

    private LookupRef ref(long id) {
        return new LookupRef("uri" + id, id, Publisher.TESTING_MBST, ContentCategory.TOP_LEVEL_ITEM);
    }



    private static Matcher<EquivalenceChangeMessage> messageMatcher(
            long subjectId,
            Set<Long> outgoingIdsAdded,
            Set<Long> outgoingIdsRemoved,
            Set<Long> outgoingIdsUnchanged,
            Set<String> sources
    ) {
        return new BaseMatcher<EquivalenceChangeMessage>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof EquivalenceChangeMessage)) {
                    return false;
                }

                EquivalenceChangeMessage msg = (EquivalenceChangeMessage) o;
                return msg.getSubjectId() == subjectId
                        && msg.getOutgoingIdsAdded().equals(outgoingIdsAdded)
                        && msg.getOutgoingIdsRemoved().equals(outgoingIdsRemoved)
                        && msg.getOutgoingIdsUnchanged().equals(outgoingIdsUnchanged)
                        && msg.getSources().equals(sources);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                        "EquivalenceChangeMessage for:" +
                                "\nsubjectId: " + subjectId +
                                "\noutgoingIdsAdded: " + outgoingIdsAdded +
                                "\noutgoingIdsRemoved: " + outgoingIdsRemoved +
                                "\noutgoingIdsUnchanged: " + outgoingIdsUnchanged +
                                "\nsources: " + sources

                );
            }
        };
    }
}