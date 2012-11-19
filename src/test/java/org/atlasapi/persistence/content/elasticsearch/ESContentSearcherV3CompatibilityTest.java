/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.persistence.content.elasticsearch;

import static org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder.broadcast;
import static org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder.complexItem;
import static org.atlasapi.media.entity.testing.VersionTestDataBuilder.version;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ESContentSearcherV3CompatibilityTest {

    private static final ImmutableSet<Publisher> ALL_PUBLISHERS = ImmutableSet.copyOf(Publisher.values());
    
    private final Brand dragonsDen = brand("/den", "Dragon's den");
    private final Item dragonsDenItem = complexItem().withBrand(dragonsDen).withVersions(broadcast().buildInVersion()).build();
    private final Brand doctorWho = brand("/doctorwho", "Doctor Who");
    private final Item doctorWhoItem = complexItem().withBrand(doctorWho).withVersions(broadcast().buildInVersion()).build();
    private final Brand theCityGardener = brand("/garden", "The City Gardener");
    private final Item theCityGardenerItem = complexItem().withBrand(theCityGardener).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastenders = brand("/eastenders", "Eastenders");
    private final Item eastendersItem = complexItem().withBrand(eastenders).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastendersWeddings = brand("/eastenders-weddings", "Eastenders Weddings");
    private final Item eastendersWeddingsItem = complexItem().withBrand(eastendersWeddings).withVersions(broadcast().buildInVersion()).build();
    private final Brand politicsEast = brand("/politics", "The Politics Show East");
    private final Item politicsEastItem = complexItem().withBrand(politicsEast).withVersions(broadcast().buildInVersion()).build();
    private final Brand meetTheMagoons = brand("/magoons", "Meet the Magoons");
    private final Item meetTheMagoonsItem = complexItem().withBrand(meetTheMagoons).withVersions(broadcast().buildInVersion()).build();
    private final Brand theJackDeeShow = brand("/dee", "The Jack Dee Show");
    private final Item theJackDeeShowItem = complexItem().withBrand(theJackDeeShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand peepShow = brand("/peep-show", "Peep Show");
    private final Item peepShowItem = complexItem().withBrand(peepShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand euromillionsDraw = brand("/draw", "EuroMillions Draw");
    private final Item euromillionsDrawItem = complexItem().withBrand(euromillionsDraw).withVersions(broadcast().buildInVersion()).build();
    private final Brand haveIGotNewsForYou = brand("/news", "Have I Got News For You");
    private final Item haveIGotNewsForYouItem = complexItem().withBrand(haveIGotNewsForYou).withVersions(broadcast().buildInVersion()).build();
    private final Brand brasseye = brand("/eye", "Brass Eye");
    private final Item brasseyeItem = complexItem().withBrand(brasseye).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand science = brand("/science", "The Story of Science: Power, Proof and Passion");
    private final Item scienceItem = complexItem().withBrand(science).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand theApprentice = brand("/apprentice", "The Apprentice");
    private final Item theApprenticeItem = complexItem().withBrand(theApprentice).withVersions(broadcast().buildInVersion()).build();
    
    private final Item apparent = complexItem().withTitle("Without Apparent Motive").withUri("/item/apparent").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item englishForCats = complexItem().withUri("/items/cats").withTitle("English for cats").withVersions(version().withBroadcasts(broadcast().build()).build()).build();
    private final Item u2 = complexItem().withUri("/items/u2").withTitle("U2 Ultraviolet").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item spooks = complexItem().withTitle("Spooks").withUri("/item/spooks")
            .withVersions(version().withBroadcasts(broadcast().withStartTime(new SystemClock().now().minus(Duration.standardDays(28))).build()).build()).build();
    private final Item spookyTheCat = complexItem().withTitle("Spooky the Cat").withUri("/item/spookythecat").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item jamieOliversCookingProgramme = complexItem().withUri("/items/oliver/1").withTitle("Jamie Oliver's cooking programme")
            .withDescription("lots of words that are the same alpha beta").withVersions(broadcast().buildInVersion()).build();
    private final Item gordonRamsaysCookingProgramme = complexItem().withUri("/items/ramsay/2").withTitle("Gordon Ramsay's cooking show").withDescription("lots of words that are the same alpha beta")
            .withVersions(broadcast().buildInVersion()).build();
    
    private final Brand rugby = brand("/rugby", "Rugby");
    private final Item rugbyItem = complexItem().withBrand(rugby).withVersions(ComplexBroadcastTestDataBuilder.broadcast().withChannel("http://minor-channel").buildInVersion()).build();
    
    private final Brand sixNationsRugby = brand("/sixnations", "Six Nations Rugby Union");
    private final Item sixNationsRugbyItem = complexItem().withBrand(sixNationsRugby).withVersions(ComplexBroadcastTestDataBuilder.broadcast().withChannel("http://www.bbc.co.uk/services/bbcone/east").buildInVersion()).build();

    private final Brand hellsKitchen = brand("/hellskitchen", "Hell's Kitchen");
    private final Item hellsKitchenItem = complexItem().withBrand(hellsKitchen).withVersions(broadcast().buildInVersion()).build();
    
    private final Brand hellsKitchenUSA = brand("/hellskitchenusa", "Hell's Kitchen");
    private final Item hellsKitchenUSAItem = complexItem().withBrand(hellsKitchenUSA).withVersions(broadcast().buildInVersion()).build();
    
    private final Item we = complexItem().withTitle("W.E.").withUri("/item/we").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final List<Brand> brands = Arrays.asList(doctorWho, eastendersWeddings, dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou,
            euromillionsDraw, brasseye, science, politicsEast, theApprentice, rugby, sixNationsRugby, hellsKitchen, hellsKitchenUSA);

    private final List<Item> items = Arrays.asList(apparent, englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, spooks, spookyTheCat, dragonsDenItem, doctorWhoItem,
            theCityGardenerItem, eastendersItem, eastendersWeddingsItem, politicsEastItem, meetTheMagoonsItem, theJackDeeShowItem, peepShowItem, euromillionsDrawItem, haveIGotNewsForYouItem,
            brasseyeItem, scienceItem, theApprenticeItem, rugbyItem, sixNationsRugbyItem, hellsKitchenItem, hellsKitchenUSAItem, we);

    private final List<Item> itemsUpdated = Arrays.asList(u2);

    private Node esClient;
    private ESContentIndexer indexer;
    private ESContentSearcher searcher;

    @Before
    public void before() throws Exception {
        esClient = NodeBuilder.nodeBuilder().local(true).clusterName(ESSchema.CLUSTER_NAME).build().start();
        try {
            esClient.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME));
        } catch (Exception ex) {
        }
        indexer = new ESContentIndexer(esClient, new SystemClock());
        searcher = new ESContentSearcher(esClient);
        //
        indexer.init();
        //
        for (Container c : brands) {
            indexer.index(c);
        }
        for (Item i : items) {
            indexer.index(i);
        }
        for (Item i : itemsUpdated) {
            indexer.index(i);
        }
        Thread.sleep(2000);
    }

    @After
    public void after() throws Exception {
        esClient.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME));
        esClient.close();
        Thread.sleep(3000);
    }

    @Test
    public void testFindingBrandsByTitle() throws Exception {
        check(searcher.search(title("aprentice")).get(), theApprentice);
        check(searcher.search(currentWeighted("apprent")).get(), theApprentice, apparent);
        check(searcher.search(title("den")).get(), dragonsDen, theJackDeeShow);
        check(searcher.search(title("dragon")).get(), dragonsDen);
        check(searcher.search(title("dragons")).get(), dragonsDen);
        check(searcher.search(title("drag den")).get(), dragonsDen);
        check(searcher.search(title("drag")).get(), dragonsDen, euromillionsDraw);
        check(searcher.search(title("dragon's den")).get(), dragonsDen);
        check(searcher.search(title("eastenders")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("easteners")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("eastedners")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("politics east")).get(), politicsEast);
        check(searcher.search(title("eas")).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("east")).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("end")).get());
        check(searcher.search(title("peep show")).get(), peepShow);
        check(searcher.search(title("peep s")).get(), peepShow);
        check(searcher.search(title("dee")).get(), theJackDeeShow, dragonsDen);
        check(searcher.search(title("jack show")).get(), theJackDeeShow);
        check(searcher.search(title("the jack dee s")).get(), theJackDeeShow);
        check(searcher.search(title("dee show")).get(), theJackDeeShow);
        check(searcher.search(title("hav i got news")).get(), haveIGotNewsForYou);
        check(searcher.search(title("brasseye")).get(), brasseye);
        check(searcher.search(title("braseye")).get(), brasseye);
        check(searcher.search(title("brassey")).get(), brasseye);
        check(searcher.search(title("The Story of Science Power Proof and Passion")).get(), science);
        check(searcher.search(title("The Story of Science: Power, Proof and Passion")).get(), science);
        check(searcher.search(title("Jamie")).get(), jamieOliversCookingProgramme);
        check(searcher.search(title("Spooks")).get(), spooks, spookyTheCat);
    }
    
    @Test
    public void testFindingBrandsByTitleAfterUpdate() throws Exception {
        check(searcher.search(title("aprentice")).get(), theApprentice);
        //
        Brand theApprentice2 = new Brand();
        Brand.copyTo(theApprentice, theApprentice2);
        theApprentice2.setTitle("Completely Different2");
        indexer.index(theApprentice2);
        Thread.sleep(2000);
        //
        checkNot(searcher.search(title("aprentice")).get(), theApprentice);
        check(searcher.search(title("Completely Different2")).get(), theApprentice);
    }
    
    @Test
    public void testFindingBrandsBySpecialization() throws Exception {
        check(searcher.search(title("aprentice")).get(), theApprentice);
        //
        Item theApprenticeItem2 = new Item();
        Item.copyTo(theApprenticeItem, theApprenticeItem2);
        theApprenticeItem2.setSpecialization(Specialization.RADIO);
        indexer.index(theApprenticeItem2);
        Thread.sleep(2000);
        //
        checkNot(searcher.search(specializedTitle("aprentice", Specialization.TV)).get(), theApprentice);
        check(searcher.search(specializedTitle("aprentice", Specialization.RADIO)).get(), theApprentice);
    }

    @Test
    public void testLimitingToPublishers() throws Exception {
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.BBC), 1.0f, 0.0f, 0.0f)).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.ARCHIVE_ORG), 1.0f, 0.0f, 0.0f)).get());

        Brand eastBrand = new Brand("/east", "curie", Publisher.ARCHIVE_ORG);
        eastBrand.setTitle("east");
        Item eastItem = new Item("/eastItem", "curie", Publisher.ARCHIVE_ORG);
        eastBrand.setTitle("east");
        eastBrand.setChildRefs(Arrays.asList(eastItem.childRef()));
        eastItem.setTitle("east");
        eastItem.setParentRef(ParentRef.parentRefFrom(eastBrand));
        indexer.index(eastBrand);
        indexer.index(eastItem);
        Thread.sleep(2000);
        
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.ARCHIVE_ORG), 1.0f, 0.0f, 0.0f)).get(), eastBrand);
    }

    @Test
    public void testUsesPrefixSearchForShortSearches() throws Exception {
        // commented out for now as order is inverted:
        //check(searcher.search(title("Dr")).get(), doctorWho, dragonsDen);
        check(searcher.search(title("l")).get());
    }

    @Test
    public void testLimitAndOffset() throws Exception {
        check(searcher.search(new SearchQuery("eas", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f)).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery("eas", Selection.limitedTo(2), ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f)).get(), eastenders, eastendersWeddings);
        check(searcher.search(new SearchQuery("eas", Selection.offsetBy(2), ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f)).get(), politicsEast);
    }

    @Test
    public void testBroadcastLocationWeighting() throws Exception {
        check(searcher.search(currentWeighted("spooks")).get(), spooks, spookyTheCat);
        // commented out for now as order is inverted:
        //check(searcher.search(title("spook")).get(), spooks, spookyTheCat);
        check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);
    }
     
    @Test
    public void testBrandWithNoChildrenIsPickedWithTitleWeighting() throws Exception {
        check(searcher.search(title("spook")).get(), spookyTheCat, spooks);

        Brand spookie = new Brand("/spookie", "curie", Publisher.ARCHIVE_ORG);
        spookie.setTitle("spookie");
        indexer.index(spookie);
        Thread.sleep(2000);
        
        check(searcher.search(title("spook")).get(), spookie, spookyTheCat, spooks);
    }
    
    @Test
    public void testBrandWithNoChildrenIsNotPickedWithBroadcastWeighting() throws Exception {
        check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);

        Brand spookie = new Brand("/spookie2", "curie", Publisher.ARCHIVE_ORG);
        spookie.setTitle("spookie2");
        indexer.index(spookie);
        Thread.sleep(2000);
        
        check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);
    }
    
    protected static SearchQuery title(String term) {
        return new SearchQuery(term, Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f);
    }
    
    protected static SearchQuery specializedTitle(String term, Specialization specialization) {
        return new SearchQuery(term, Selection.ALL, Sets.newHashSet(specialization), ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f);
    }

    protected static SearchQuery currentWeighted(String term) {
        return new SearchQuery(term, Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.2f, 0.2f);
    }

    protected static void check(SearchResults result, Identified... content) {
        assertThat(result.toUris(), is(toUris(Arrays.asList(content))));
    }
    
    protected static void checkNot(SearchResults result, Identified... content) {
        assertFalse(result.toUris().equals(toUris(Arrays.asList(content))));
    }

    protected static Brand brand(String uri, String title) {
        Brand b = new Brand(uri, uri, Publisher.BBC);
        b.setTitle(title);
        return b;
    }

    protected static Item item(String uri, String title) {
        return item(uri, title, null);
    }

    protected static Item item(String uri, String title, String description) {
        Item i = new Item();
        i.setTitle(title);
        i.setCanonicalUri(uri);
        i.setDescription(description);
        i.setPublisher(Publisher.BBC);
        return i;
    }

    protected static Person person(String uri, String title) {
        Person p = new Person(uri, uri, Publisher.BBC);
        p.setTitle(title);
        return p;
    }
    
    private static List<String> toUris(List<? extends Identified> content) {
        List<String> uris = Lists.newArrayList();
        for (Identified description : content) {
            uris.add(description.getCanonicalUri());
        }
        return uris;
    }
}
