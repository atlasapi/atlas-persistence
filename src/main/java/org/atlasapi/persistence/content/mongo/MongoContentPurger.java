package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentPurger;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MongoContentPurger implements ContentPurger {

    private static final Logger log = LoggerFactory.getLogger(MongoContentPurger.class);
    
    static final String ATLAS_EQUIVALENCE_ALIAS = "atlas:equivalence";
    
    private final ContentLister contentLister;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final MongoContentTables contentTables;
    private final DBCollection lookupCollection;
    private final LookupWriter explicitTransitiveLookupWriter;
    private final LookupWriter generatedTransitiveLookupWriter;
    
    public MongoContentPurger(ContentLister contentLister, ContentResolver contentResolver,
                ContentWriter contentWriter, MongoContentTables contentTables,
                DBCollection lookupCollection, LookupWriter explicitTransitiveLookupWriter, 
                LookupWriter generatedTransitiveLookupWriter) {
        this.lookupCollection = checkNotNull(lookupCollection);
        this.contentTables = checkNotNull(contentTables);
        this.contentLister = checkNotNull(contentLister);
        this.contentResolver = checkNotNull(contentResolver);
        this.contentWriter = checkNotNull(contentWriter);
        this.explicitTransitiveLookupWriter = checkNotNull(explicitTransitiveLookupWriter);
        this.generatedTransitiveLookupWriter = checkNotNull(generatedTransitiveLookupWriter);
    }
    
    @Override
    public void purge(Publisher publisher, Set<Publisher> equivalencesToRetainAsAliases) {
        Iterator<Content> contentIterator = 
                contentLister.listContent(
                                    new ContentListingCriteria.Builder()
                                                              .forPublisher(publisher)
                                                              .build()
                                    );
        
        while (contentIterator.hasNext()) {
            Content c = contentIterator.next();
            retainEquivalencesAsAliases(c, equivalencesToRetainAsAliases);
            removeAllEquivalences(c);
            deleteContent(c);
        }

    }

    private void deleteContent(Content c) {
        DBCollection contentCollection = contentTables.collectionFor(ContentCategory.categoryFor(c));
        DBObject criteria = BasicDBObjectBuilder.start("_id", c.getCanonicalUri()).get();
        contentCollection.remove(criteria);
        lookupCollection.remove(criteria);
    }

    private void removeAllEquivalences(Content c) {
        explicitTransitiveLookupWriter.writeLookup(ContentRef.valueOf(c), ImmutableSet.<ContentRef>of(), Publisher.all());
        generatedTransitiveLookupWriter.writeLookup(ContentRef.valueOf(c), ImmutableSet.<ContentRef>of(), Publisher.all());
    }
    
    private void retainEquivalencesAsAliases(Content c,
            Set<Publisher> equivalencesToRetainAsAliases) {
        
        for (LookupRef lookupRef : c.getEquivalentTo()) {
            if (equivalencesToRetainAsAliases.contains(lookupRef.publisher())) {
                Identified equiv = contentResolver.findByCanonicalUris(ImmutableSet.of(lookupRef.uri()))
                                                  .getFirstValue()
                                                  .requireValue();
                equiv.addAlias(new Alias(ATLAS_EQUIVALENCE_ALIAS, c.getCanonicalUri()));
                write(equiv);
            }
        }
        
    }
    
    private void write(Identified identified) {
        if (identified instanceof Item) {
            contentWriter.createOrUpdate((Item) identified);
        } else if (identified instanceof Container) {
            contentWriter.createOrUpdate((Container) identified);
        } else {
            throw new IllegalArgumentException("Can't write " + identified.getCanonicalUri());
        }
    }
    
    @Override
    public void restoreEquivalences(Publisher publisher) {
        Iterator<Content> contentIterator = contentLister.listContent(
                                new ContentListingCriteria.Builder()
                                                          .forPublisher(publisher)
                                                          .build()
                                                 );
        
        while (contentIterator.hasNext()) {
            Content content = contentIterator.next();
            for (Alias alias : Iterables.filter(content.getAliases(), IS_EQUIVALENCE_ALIAS)) {
                Maybe<Identified> contentFromPurgedPublisher = 
                        contentResolver.findByCanonicalUris(ImmutableSet.of(alias.getValue())).getFirstValue();
                
                if (contentFromPurgedPublisher.hasValue()) {
                    content.addEquivalentTo((Described) contentFromPurgedPublisher.requireValue());
                    HashSet<Alias> aliases = Sets.newHashSet(content.getAliases());
                    aliases.remove(alias);
                    content.setAliases(aliases);
                    write(content);
                } else {
                    log.warn("Could not find equivalent content of {} for {} so not writing equivalence", 
                            alias.getValue(), content.getCanonicalUri());
                }
            }
        }
    }
    
    private final Predicate<Alias> IS_EQUIVALENCE_ALIAS = new Predicate<Alias>() {

        @Override
        public boolean apply(Alias alias) {
            return ATLAS_EQUIVALENCE_ALIAS.equals(alias.getNamespace());
        }
        
    };

}
