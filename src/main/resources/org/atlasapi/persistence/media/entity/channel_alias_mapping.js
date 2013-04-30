var aliasMapping = new Object();

aliasMapping[escapeRegExp("http://pressassociation.com/channels/") + "([0-9]*)"] = {"namespace" : "pa:channel-id", "value" : "1"};
aliasMapping[escapeRegExp("http://pressassociation.com/stations/") + "([0-9]*)"] = {"namespace" : "pa:station-id", "value" : "1"};
aliasMapping[escapeRegExp("http://pressassociation.com/regions/") + "([0-9]*)"] = {"namespace" : "pa:region-id", "value" : "1"};
aliasMapping[escapeRegExp("http://pressassociation.com/platforms/") + "([0-9]*)"] = {"namespace" : "pa:platform-id", "value" : "1"};
aliasMapping[escapeRegExp("http://youview.com/service/") + "([0-9]*)"] = {"namespace" : "youview:service-id", "value" : "1"};
aliasMapping[escapeRegExp("http://xmltv.radiotimes.com/channels/") + "([0-9]*)"] = {"namespace" : "xmltv:channel", "value" : "1"};

aliasMapping[escapeRegExp("http://devapi.bbcredux.com/channels/") + "([a-z0-9]*)"] = {"namespace" : "bbcredux:service-id", "value" : "1"};

aliasMapping[escapeRegExp("http://ref.atlasapi.org/channels/") + "([a-z]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.bbc.co.uk/services/") + "([a-z0-9]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.e4.com/") + "?" + "([a-z]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://film4.com")] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.4music.com")] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.channel4.com/") + "?" + "([a-z0-9#]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.bbc.co.uk/iplayer")] = {"namespace" : "", "value" : "1"};
	
aliasMapping[escapeRegExp("http://www.hulu.com")] = {"namespace" : "", "value" : "1"};
		
aliasMapping[escapeRegExp("http://www.youtube.com")] = {"namespace" : "", "value" : "1"};
			
aliasMapping[escapeRegExp("http://www.seesaw.com")] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://tvblob.com/channel/") + "([a-z0-9]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.five.tv/channels") + "([a-z0-9_]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.five.tv")] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://www.itv.com/channels/") + "([a-z0-9]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping["^[^:]*$"] = {"namespace" : "", "value" : "1"};

db.channels.find().forEach(function(c){findAllAliases(c);});
db.channelGroups.find().forEach(function(c){findAllAliases(c);});

function findAllAliases(c) {
	if (!c.publisher) {
		return;
	}
	if (c.publisher.indexOf("metabroadcast.com") === -1) {
		return;
	}
	
	if (typeof(c._id) != "object") {
		findAlias(c._id);
//	} else {
//		print(c._id);
	}
	if (c.uri) {
		findAlias(c.uri);
	}
	if (c.aliases) {
		for (var i = 0; i < c.aliases.length; i++) {
			findAlias(c.aliases[i]);
		}
	}
}

function escapeRegExp(unescaped){
	return unescaped.replace(/([.*+?^=!:${}()|[\]\/\\])/g, "\\$1");
}

function findAlias(uri) {
	// iterate over all regexes, try and match one
	var found = false;
	uri = uri.trim();
	
	for (var aliasRegex in aliasMapping) {
		var re = new RegExp(aliasRegex);
		if (re.test(uri)) {
			found = true;
//			print("matched " + uri + " to " + aliasRegex);
			break;
		} 
	}
	
	if (!found) {
		print("no match found for: " + uri + "\n");
	}
}