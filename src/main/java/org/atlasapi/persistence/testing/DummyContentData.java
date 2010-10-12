/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.persistence.testing;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Description;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;

public class DummyContentData {

	public static final DateTime april22nd1730 = new DateTime(2009, 04, 22, 17, 30, 00, 000, DateTimeZone.UTC);
	public static final DateTime april22nd1930 = new DateTime(2009, 04, 22, 19, 30, 00, 000, DateTimeZone.UTC);
	public static final DateTime april23rd = new DateTime(2009, 04, 23, 19, 30, 00, 000, DateTimeZone.UTC);
	public static final DateTime now = new SystemClock().now();
	
	public Brand eastenders;
	public Episode dotCottonsBigAdventure;
	public Episode peggySlapsFrank;
	
	public Playlist goodEastendersEpisodes;
	
	public Playlist mentionedOnTwitter;
	
	public Brand apprentice;
	public Item sellingStuff;

	public Brand thickOfIt;
	public Item mpSaysSomethingFunny;

	
	public Brand timeTeam;
	public Item fossils;
	
	public Brand ER;
	public Episode brainSurgery;

	
	public Brand newsNight;
	public Item interviewWithMp;
	
	public Brand neighbours;
	public Item susanAndCarlGoFishing;
	
	public Brand dispatches;
	public Item theCreditCrunch;
	
	public Brand eelFishing;
	public Item everyoneNeedsAnEel;

	public Item englishForCats;
	public Item eggsForBreakfast;

	public DummyContentData() {
		
		/* Eastenders */
		dotCottonsBigAdventure = new Episode("http://www.bbc.co.uk/eastenders/1", "bbc:eastenders:1", Publisher.BBC);
		dotCottonsBigAdventure.setEpisodeNumber(1);
		dotCottonsBigAdventure.setTitle("Dot Cotton's Big Adventure");
		dotCottonsBigAdventure.addAlias("http://dot.cotton");
		dotCottonsBigAdventure.addVersion(versionWithEmbeddableLocation(april22nd1930, Duration.standardSeconds(30), TransportType.STREAM));
		dotCottonsBigAdventure.setTags(Sets.newHashSet("soap", "London"));
		dotCottonsBigAdventure.setGenres(Sets.newHashSet("http://ref.atlasapi.org/genres/atlas/drama"));
		dotCottonsBigAdventure.setIsLongForm(true);
		dotCottonsBigAdventure.setLastUpdated(april23rd);

		
		peggySlapsFrank = new Episode("http://www.bbc.co.uk/eastenders/2", "bbc:eastenders:2", Publisher.BBC);
		peggySlapsFrank.setEpisodeNumber(2);
		peggySlapsFrank.setTitle("Episode where Peggy slaps Frank");
		peggySlapsFrank.addVersion(versionWithEmbeddableLocation(april23rd, Duration.standardSeconds(30), TransportType.STREAM));
		peggySlapsFrank.setTags(Sets.newHashSet("soap", "London"));
		peggySlapsFrank.setGenres(Sets.newHashSet("http://ref.atlasapi.org/genres/atlas/drama"));
		peggySlapsFrank.setIsLongForm(true);

		
		eastenders = new Brand("http://www.bbc.co.uk/eastenders", "bbc:eastenders", Publisher.BBC);
		eastenders.setDescription("Gritty drama etc.");
		eastenders.setTitle("Eastenders");
		eastenders.addItems(dotCottonsBigAdventure, peggySlapsFrank);
		eastenders.addAlias("http://eastenders.bbc");
		
		goodEastendersEpisodes = new Playlist("http://www.bbc.co.uk/eastenders/good", "bbc:east-good", Publisher.BBC);
		goodEastendersEpisodes.setTitle("EastEnders: the best bits");
		goodEastendersEpisodes.addItem(dotCottonsBigAdventure);
		
		sellingStuff = new Item("http://www.bbc.co.uk/apprentice/1", "bbc:apprentice:1", Publisher.BBC);
		sellingStuff.addVersion(versionWithEmbeddableLocation());
		
		apprentice = new Brand("http://www.bbc.co.uk/apprentice", "bbc:apprentice", Publisher.BBC);
		apprentice.setDescription("With Sir Alan Sugar");
		apprentice.setTitle("The Apprentice");
		apprentice.addItem(sellingStuff);
		
		mpSaysSomethingFunny = new Item("http://www.bbc.co.uk/thethickofit/1", "bbc:political:1", Publisher.BBC);
		mpSaysSomethingFunny.addVersion(versionWithEmbeddableLocation());
		
		thickOfIt = new Brand("http://www.bbc.co.uk/thethickofit", "bbc:political:1", Publisher.BBC);
		thickOfIt.setDescription("political comedy");
		thickOfIt.setTitle("The Thick of It");
		thickOfIt.addItem(mpSaysSomethingFunny);
		
		fossils = new Item("http://www.channel4.com/timeteam/fossil", "c4:timeteam:1", Publisher.C4);
		fossils.addVersion(versionWithEmbeddableLocation());
		
		timeTeam = new Brand("http://www.channel4.com/timeteam", "c4:timeteam", Publisher.C4);
		timeTeam.setDescription("archealogical comedy");
		timeTeam.setTitle("Time Team");
		timeTeam.addItem(fossils);
		
		brainSurgery = new Episode("http://www.channel4.com/surgery", "c4:surgery", Publisher.C4);
		brainSurgery.addVersion(versionWithEmbeddableLocation());
		brainSurgery.setTitle("Brain Surgery");
		
		ER = new Brand("http://www.channel4.com/er", "c4:er", Publisher.C4);
		ER.setDescription("medical drama");
		ER.setTitle("ER");
		ER.addItem(brainSurgery);
		
		/* Newsnight */
		
		interviewWithMp = new Episode("http://www.bbc.co.uk/newsnight/1", "bbc:newsnight:1", Publisher.BBC);
		interviewWithMp.setTitle("Interview with MP");
		interviewWithMp.addVersion(versionWithEmbeddableLocation(now, Duration.standardSeconds(60), TransportType.LINK));
		interviewWithMp.setIsLongForm(true);

		
		newsNight = new Brand("http://www.bbc.co.uk/newsnight", "bbc:newsnight", Publisher.BBC);
		newsNight.setDescription("Interviews");
		newsNight.setTitle("Newsnight");
		newsNight.addItems( interviewWithMp);
		
		/* Neighbours */
		susanAndCarlGoFishing = new Episode("http://www.five.tv/neighbours/82883883", "c5:82883883", Publisher.FIVE);
		susanAndCarlGoFishing.setTitle("Susan and Carl Go Fishing");
		susanAndCarlGoFishing.addVersion(versionWithEmbeddableLocation(april22nd1730, Duration.standardSeconds(30), TransportType.LINK));
		susanAndCarlGoFishing.setTags(Sets.newHashSet("soap", "aussie"));
		susanAndCarlGoFishing.addVersion(versionWithEmbeddableLocation());

		neighbours = new Brand("http://www.five.tv/neighbours", "c5:neighbours", Publisher.FIVE);
		neighbours.setDescription("Teatime favourite");
		neighbours.setTitle("Neighbours");
		neighbours.addItems(susanAndCarlGoFishing);
		
		/* Eel fishing */
		everyoneNeedsAnEel = new Episode("http://www.bbc.co.uk/eels", "bbc:eels", Publisher.BBC);
		everyoneNeedsAnEel.setTitle("everyone needs an eel");
		everyoneNeedsAnEel.setTags(Sets.newHashSet("eel"));
		everyoneNeedsAnEel.setGenres(Sets.newHashSet("nature", "eels"));
		everyoneNeedsAnEel.addVersion(versionWithEmbeddableLocation());
		
		eelFishing = new Brand("http://www.bbc.co.uk/eels/1", "bbc:eels:1", Publisher.BBC);
		eelFishing.setDescription("Classic marine drama");
		eelFishing.setTitle("eel Fishing");
		eelFishing.addItems(everyoneNeedsAnEel);
		
		/* Dispatches */
		theCreditCrunch = new Episode("http://www.channel4.com/dispatches/25", "c4:dispatches:25", Publisher.C4);
		theCreditCrunch.setTitle("How the Credit Crunch has affected migratory Sea Bass");
		theCreditCrunch.setTags(Sets.newHashSet("fish", "creditcrunch"));
		theCreditCrunch.setGenres(Sets.newHashSet("Documentary", "Money"));
		theCreditCrunch.addVersion(versionWithEmbeddableLocation());

		
		dispatches = new Brand("http://www.channel4.com/dispatches", "c4:dispatches", Publisher.C4);
		dispatches.setDescription("Hard-hitting investigative show");
		dispatches.setTitle("Dispatches");
		dispatches.addItems(theCreditCrunch);
		
		/* A user-generated video */
		englishForCats = new Item("http://www.youtube.co.uk/watch?v=1234", "yt:1234", Publisher.YOUTUBE);
		englishForCats.setTitle("English for Cats");
		englishForCats.setDescription("A wonderful exposition of feline-accessible literature");
		englishForCats.setGenres(Sets.newHashSet("http://ref.atlasapi.org/genres/atlas/drama"));
		englishForCats.addVersion(versionWithEmbeddableLocation());
		englishForCats.setIsLongForm(true);
		
		/* A user-generated video */
		eggsForBreakfast = new Item("http://www.youtube.co.uk/watch?v=12345", "yt:12345", Publisher.YOUTUBE);
		eggsForBreakfast.setTitle("eggs for breakfast");
		eggsForBreakfast.setDescription("Breakfast");
		eggsForBreakfast.addVersion(versionWithEmbeddableLocation());
		
		
		mentionedOnTwitter = new Playlist("http://ref.atlasapi.org/mentions/twitter", "twitter:mentions");
		mentionedOnTwitter.setPublisher(Publisher.DAILYMOTION);
		mentionedOnTwitter.setTitle("Mentioned on twitter");
		mentionedOnTwitter.addItem(englishForCats);
		mentionedOnTwitter.addItem(eggsForBreakfast);
		mentionedOnTwitter.addPlaylist(eastenders);
		mentionedOnTwitter.addPlaylist(newsNight);
	}
	
	public Version versionWithEmbeddableLocation() {
		return versionWithEmbeddableLocation(now, Duration.standardSeconds(10), TransportType.LINK);
	}
	
	public static Version versionWithEmbeddableLocation(DateTime date, Duration duration, TransportType type) {
		Version v = new Version();
		Broadcast broadcast = new Broadcast("test channel", date, date.plusHours(1));
		v.addBroadcast(broadcast);
		v.setDuration(duration);
		Encoding e = new Encoding();
		Location l = new Location();
		v.addManifestedAs(e);
		e.addAvailableAt(l);
		l.setTransportType(type);
		return v;
	}

	public static String uriOf(Description brand) {
		return brand.getCanonicalUri();
	}
}
