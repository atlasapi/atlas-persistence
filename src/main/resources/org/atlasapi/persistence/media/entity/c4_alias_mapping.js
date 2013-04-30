var aliasMapping = new Object();

//aliasMapping[escapeRegExp("tag:www.channel4.com,") + "([0-9]{4})" + escapeRegExp(":/programmes/") + "([a-z0-4-]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://www.channel4.com/") + "([a-z0-4]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping["(" + escapeRegExp("http://www.channel4.com") + ")"] = {"namespace" : "", "value" : "1"};

//aliasMapping["(" + escapeRegExp("http://www.4music.com") + ")"] = {"namespace" : "", "value" : "1"};

//aliasMapping[escapeRegExp("tag:www.e4.com,") + "([0-9]{4})" + escapeRegExp(":/programmes/") + "([a-z0-4-]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping[escapeRegExp("http://www.e4.com/") + "([a-z0-4]*)"] = {"namespace" : "", "value" : "1"};
//aliasMapping["(" + escapeRegExp("http://www.e4.com") + ")"] = {"namespace" : "", "value" : "1"};

//aliasMapping["(" + escapeRegExp("http://film4.com") + ")"] = {"namespace" : "", "value" : "1"};

//db.children.find().forEach(function(c){findAllAliases(c);});
//db.topLevelItems.find().forEach(function(c){findAllAliases(c);});
db.containers.find().forEach(function(c){findAllAliases(c);});
//db.programmeGroups.find().forEach(function(c){findAllAliases(c);});
//db.contentGroups.find().forEach(function(c){findAllAliases(c);});
//db.people.find().forEach(function(c){findAllAliases(c);});

function findAllAliases(c) {
	if (c.publisher.indexOf("channel4.com") === -1) {
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