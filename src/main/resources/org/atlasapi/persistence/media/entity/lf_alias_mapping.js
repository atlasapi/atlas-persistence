var aliasMapping = new Object();

aliasMapping[escapeRegExp("http://lovefilm.com/shows/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://lovefilm.com/seasons/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://lovefilm.com/episodes/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};
aliasMapping[escapeRegExp("http://lovefilm.com/films/") + "([0-9]*)"] = {"namespace" : "", "value" : "1"};

db.children.find().forEach(function(c){findAllAliases(c);});
db.topLevelItems.find().forEach(function(c){findAllAliases(c);});
db.containers.find().forEach(function(c){findAllAliases(c);});

function findAllAliases(c) {
	if (c.publisher.indexOf("lovefilm.com") === -1) {
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