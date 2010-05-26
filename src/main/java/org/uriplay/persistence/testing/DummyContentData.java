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

package org.uriplay.persistence.testing;

import org.joda.time.DateTime;
import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Broadcast;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;

import com.google.common.collect.Sets;

public class DummyContentData {

	public static final DateTime april22nd1730 = new DateTime(2009, 04, 22, 17, 30, 00, 000);
	public static final DateTime april22nd1930 = new DateTime(2009, 04, 22, 19, 30, 00, 000);
	public static final DateTime april23rd = new DateTime(2009, 04, 23, 19, 30, 00, 000);
	
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
		dotCottonsBigAdventure = new Episode("http://www.bbc.co.uk/eastenders/1", "bbc:eastenders:1");
		dotCottonsBigAdventure.setEpisodeNumber(1);
		dotCottonsBigAdventure.setTitle("Dot Cotton's Big Adventure");
		dotCottonsBigAdventure.setPublisher("bbc.co.uk");
		dotCottonsBigAdventure.addAlias("http://dot.cotton");
		dotCottonsBigAdventure.addVersion(versionWithEmbeddableLocation(april22nd1930, 30, TransportType.STREAM));
		dotCottonsBigAdventure.setTags(Sets.newHashSet("soap", "London"));
		dotCottonsBigAdventure.setGenres(Sets.newHashSet("http://uriplay.org/genres/uriplay/drama"));
		dotCottonsBigAdventure.setIsLongForm(true);

		
		peggySlapsFrank = new Episode("http://www.bbc.co.uk/eastenders/2", "bbc:eastenders:2");
		peggySlapsFrank.setEpisodeNumber(2);
		peggySlapsFrank.setTitle("Episode where Peggy slaps Frank");
		peggySlapsFrank.setPublisher("bbc.co.uk");
		peggySlapsFrank.addVersion(versionWithEmbeddableLocation(april23rd, 30, TransportType.STREAM));
		peggySlapsFrank.setTags(Sets.newHashSet("soap", "London"));
		peggySlapsFrank.setGenres(Sets.newHashSet("http://uriplay.org/genres/uriplay/drama"));
		peggySlapsFrank.setIsLongForm(true);

		
		eastenders = new Brand("http://www.bbc.co.uk/eastenders", "bbc:eastenders");
		eastenders.setDescription("Gritty drama etc.");
		eastenders.setPublisher("bbc.co.uk");
		eastenders.setTitle("Eastenders");
		eastenders.addItems(dotCottonsBigAdventure, peggySlapsFrank);
		eastenders.addAlias("http://eastenders.bbc");
		
		goodEastendersEpisodes = new Playlist("http://www.bbc.co.uk/eastenders/good", "bbc:east-good");
		goodEastendersEpisodes.setTitle("EastEnders: the best bits");
		goodEastendersEpisodes.addItem(dotCottonsBigAdventure);
		
		sellingStuff = new Item();
		sellingStuff.addVersion(versionWithEmbeddableLocation());
		
		apprentice = new Brand("http://www.bbc.co.uk/apprentice", "bbc:apprentice");
		apprentice.setPublisher("bbc.co.uk");
		apprentice.setDescription("With Sir Alan Sugar");
		apprentice.setTitle("The Apprentice");
		apprentice.addItem(sellingStuff);
		
		mpSaysSomethingFunny = new Item();
		mpSaysSomethingFunny.addVersion(versionWithEmbeddableLocation());
		
		thickOfIt = new Brand("http://www.bbc.co.uk/thethickofit", "bbc:political:1");
		thickOfIt.setDescription("political comedy");
		thickOfIt.setTitle("The Thick of It");
		thickOfIt.addItem(mpSaysSomethingFunny);
		
		fossils = new Item();
		fossils.addVersion(versionWithEmbeddableLocation());
		
		timeTeam = new Brand("http://www.channel4.com/timeteam", "c4:timeteam");
		timeTeam.setDescription("archealogical comedy");
		timeTeam.setTitle("Time Team");
		timeTeam.addItem(fossils);
		
		brainSurgery = new Episode("http://www.channel4.com/surgery", "c4:surgery");
		brainSurgery.addVersion(versionWithEmbeddableLocation());
		brainSurgery.setPublisher("channel4.com");
		brainSurgery.setTitle("Brain Surgery");
		
		ER = new Brand("http://www.channel4.com/er", "c4:er");
		ER.setDescription("medical drama");
		ER.setTitle("ER");
		ER.addItem(brainSurgery);
		
		/* Newsnight */
		
		interviewWithMp = new Episode("http://www.bbc.co.uk/newsnight/1", "bbc:newsnight:1");
		interviewWithMp.setTitle("Interview with MP");
		interviewWithMp.setPublisher("bbc.co.uk");
		interviewWithMp.addVersion(versionWithEmbeddableLocation(new DateTime(), 60, TransportType.HTMLEMBED));
		interviewWithMp.setIsLongForm(true);

		
		newsNight = new Brand("http://www.bbc.co.uk/newsnight", "bbc:newsnight");
		newsNight.setDescription("Interviews");
		newsNight.setPublisher("bbc.co.uk");
		newsNight.setTitle("Newsnight");
		newsNight.addItems( interviewWithMp);
		
		/* Neighbours */
		susanAndCarlGoFishing = new Episode("http://www.five.tv/neighbours/82883883", "c5:82883883");
		susanAndCarlGoFishing.setTitle("Susan and Carl Go Fishing");
		susanAndCarlGoFishing.setPublisher("five.tv");
		susanAndCarlGoFishing.addVersion(versionWithEmbeddableLocation(april22nd1730, 30, TransportType.HTMLEMBED));
		susanAndCarlGoFishing.setTags(Sets.newHashSet("soap", "aussie"));
		susanAndCarlGoFishing.addVersion(versionWithEmbeddableLocation());

		neighbours = new Brand("http://www.five.tv/neighbours", "c5:neighbours");
		neighbours.setDescription("Teatime favourite");
		neighbours.setTitle("Neighbours");
		neighbours.addItems(susanAndCarlGoFishing);
		
		/* Eel fishing */
		everyoneNeedsAnEel = new Episode("http://www.bbc.co.uk/eels", "bbc:eels");
		everyoneNeedsAnEel.setTitle("everyone needs an eel");
		everyoneNeedsAnEel.setPublisher("bbc.co.uk");
		everyoneNeedsAnEel.setTags(Sets.newHashSet("eel"));
		everyoneNeedsAnEel.setGenres(Sets.newHashSet("nature", "eels"));
		everyoneNeedsAnEel.addVersion(versionWithEmbeddableLocation());
		
		eelFishing = new Brand("http://www.bbc.co.uk/eels/1", "bbc:eels:1");
		eelFishing.setDescription("Classic marine drama");
		eelFishing.setTitle("eel Fishing");
		eelFishing.addItems(everyoneNeedsAnEel);
		
		/* Dispatches */
		theCreditCrunch = new Episode("http://www.channel4.com/dispatches/25", "c4:dispatches:25");
		theCreditCrunch.setTitle("How the Credit Crunch has affected migratory Sea Bass");
		theCreditCrunch.setPublisher("channel4.com");
		theCreditCrunch.setTags(Sets.newHashSet("fish", "creditcrunch"));
		theCreditCrunch.setGenres(Sets.newHashSet("Documentary", "Money"));
		theCreditCrunch.addVersion(versionWithEmbeddableLocation());

		
		dispatches = new Brand("http://www.channel4.com/dispatches", "c4:dispatches");
		dispatches.setDescription("Hard-hitting investigative show");
		dispatches.setTitle("Dispatches");
		dispatches.addItems(theCreditCrunch);
		
		/* A user-generated video */
		englishForCats = new Item("http://www.youtube.co.uk/watch?v=1234", "yt:1234");
		englishForCats.setTitle("English for Cats");
		englishForCats.setDescription("A wonderful exposition of feline-accessible literature");
		englishForCats.setPublisher("youtube.com");
		englishForCats.setGenres(Sets.newHashSet("http://uriplay.org/genres/uriplay/drama"));
		englishForCats.addVersion(versionWithEmbeddableLocation());
		englishForCats.setIsLongForm(true);
		
		/* A user-generated video */
		eggsForBreakfast = new Item("http://www.youtube.co.uk/watch?v=12345", "yt:12345");
		eggsForBreakfast.setTitle("eggs for breakfast");
		eggsForBreakfast.setDescription("Breakfast");
		eggsForBreakfast.setPublisher("youtube.com");
		eggsForBreakfast.addVersion(versionWithEmbeddableLocation());
		
		
		mentionedOnTwitter = new Playlist("http://uriplay.org/mentions/twitter", "twitter:mentions");
		mentionedOnTwitter.setTitle("Mentioned on twitter");
		mentionedOnTwitter.addItem(englishForCats);
		mentionedOnTwitter.addItem(eggsForBreakfast);
		mentionedOnTwitter.addPlaylist(eastenders);
		mentionedOnTwitter.addPlaylist(newsNight);
	}
	
	public Version versionWithEmbeddableLocation() {
		return versionWithEmbeddableLocation(new DateTime(), 10, TransportType.HTMLEMBED);
	}
	
	public static Version versionWithEmbeddableLocation(DateTime date, int duration, TransportType type) {
		Version v = new Version();
		Broadcast broadcast = new Broadcast();
		broadcast.setTransmissionTime(date);
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
