var aliasMapping = new Object();
//http://www.bbc.co.uk/cbbc/sja/ /channel/program? vs /genre/brand
//aliasMapping[escapeRegExp("http://www.bbc.co.uk/iplayer/search/?q=") + "([a-zA-Z0-9%]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://www.bbc.co.uk/programmes/") + "([0-9a-z]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://www.bbc.co.uk/iplayer/episode/") + "([0-9a-z]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://www.bbc.co.uk/people/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://www.bbc.co.uk/services/") + "([a-z0-9]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping["(" + escapeRegExp("http://www.bbc.co.uk/iplayer") + ")"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://www.bbc.co.uk/") + "([a-z]*)" + escapeRegExp("/") + "([a-z]*)" + escapeRegExp("/")] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://www.bbc.co.uk/") + "([a-z]*)" + escapeRegExp("/") + "([a-z]*)" + escapeRegExp("/episodes/") + "([a-z]*)" + escapeRegExp("/")] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://bbc.co.uk/i/") + "([a-z0-9]*)" + escapeRegExp("/")] = {"namespace" : "", "value" : "1"};

//aliasMapping[escapeRegExp("http://devapi.bbcredux.com/channels/") + "([a-z0-9]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://g.bbcredux.com/programme/") + "([a-z0-9]*)" + escapeRegExp("/") + "([a-z0-9/]*)" + escapeRegExp("/") + "2011-07-12/" + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://g.bbcredux.com/programme/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};

aliasMapping[escapeRegExp("http://wsarchive.bbc.co.uk/brands/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://wsarchive.bbc.co.uk/episodes/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};

db.children.find().forEach(function(c){findAllAliases(c);});
db.topLevelItems.find().forEach(function(c){findAllAliases(c);});
db.containers.find().forEach(function(c){findAllAliases(c);});
db.programmeGroups.find().forEach(function(c){findAllAliases(c);});
db.contentGroups.find().forEach(function(c){findAllAliases(c);});
db.people.find().forEach(function(c){findAllAliases(c);});

function findAllAliases(c) {
	if (c.publisher.indexOf("bbc") === -1) {
		return;
	}
	
	if (typeof(c._id) != "object") {
		findAlias(c._id);
	} else {
		print(c._id);
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