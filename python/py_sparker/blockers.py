import re
from objects import Profile, KeyValue, BlockClean, BlockDirty

class TokenBlocking(object):
	@staticmethod
	def createBlocksClusters(profiles, clusters, separatorIDs = [], keysToExclude = [], clusterNameSeparator = "_"):
		defaultClusterID = max(map(lambda c: c.clusterID, clusters))
		entropies = dict(map(lambda cluster : (cluster.clusterID, cluster.entropy), clusters))
		clusterMap = dict([(k, c.clusterID) for c in clusters for k in c.keys])

		def createKeys(profile):
			try:
				dataset = str(profile.sourceId) + str(clusterNameSeparator)
				nonEmptyAttributes = filter(lambda a: not a.isEmpty(), profile.attributes)
				validKeys = filter(lambda x : len(x[1]) > 0, map(lambda x : ("","") if x.key in keysToExclude else (x.key, x.value), nonEmptyAttributes))
				split = map(lambda x: (x[0], re.split('\W+', x[1].lower())), validKeys)

				def associateCluster(data):
					key = dataset+str(data[0])
					clusterID = str(clusterMap.get(key, defaultClusterID))
					tokensOk = filter(lambda x: len(x) > 0, map(lambda x: x.strip(), data[1]))
					return map(lambda x: x+"_"+clusterID, tokensOk)
				tokens = [t for s in split for t in associateCluster(s)]
				return (profile.profileID, list(set(tokens)))
			except:
				return (profile.profileID, [])

		tokensPerProfile = profiles.map(createKeys)
		
		tokenProfile = tokensPerProfile.flatMap(lambda x: [(y, x[0]) for y in x[1]])
		
		profilePerKey = tokenProfile.groupByKey().filter(lambda x: len(x[1]) > 1)

		def associateEntropy(data):
			key = data[0]
			profiles = data[1]
			entropy = 1.0
			try:
				clusterID = int(key.split("_")[-1])
				entropy = entropies.get(clusterID, 1.0)
			except:
				pass

			return (key, profiles, entropy)

		profilesGrouped = profilePerKey.map(associateEntropy).map(lambda c: ([set(c[1])], c[2]) if len(separatorIDs) == 0 else (TokenBlocking.separateProfiles(c[1], separatorIDs), c[2]))
		profilesGroupedWithIds = profilesGrouped.filter(lambda block: len(block[0][0]) > 1 if len(separatorIDs) == 0 else len(filter(lambda b: len(b) > 0, block[0])) > 1).zipWithIndex()
		return profilesGroupedWithIds.map(lambda b: BlockDirty(b[1], b[0][0], entropy = b[0][1]) if len(separatorIDs) == 0 else BlockClean(b[1], b[0][0], entropy = b[0][1]))

	@staticmethod
	def createBlocks(profiles, separatorIDs = [], keysToExclude = []):
		tokensPerProfile = profiles.map(lambda profile : (profile.profileID, set(TokenBlocking.createKeysFromProfileAttributes(profile.attributes, keysToExclude))))
		tokenProfile = tokensPerProfile.flatMap(lambda x: [(y, x[0]) for y in x[1]])
		profilePerKey = tokenProfile.groupByKey().filter(lambda x: len(x[1]) > 1)
		profilesGrouped = profilePerKey.map(lambda c: [set(c[1])] if len(separatorIDs) == 0 else TokenBlocking.separateProfiles(c[1], separatorIDs))
		profilesGroupedWithIds = profilesGrouped.filter(lambda block: len(block[0]) > 1 if len(separatorIDs) == 0 else len(filter(lambda b: len(b) > 0, block)) > 1).zipWithIndex()
		return profilesGroupedWithIds.map(lambda b: BlockDirty(b[1], b[0]) if len(separatorIDs) == 0 else BlockClean(b[1], b[0]))
	
	@staticmethod
	def separateProfiles(elements, separators):
		inputE = elements
		output = []
		for sep in separators:
			a = [x for x in inputE if x <= sep]
			inputE = [x for x in inputE if x > sep]
			output.append(a)
		output.append(inputE)
		return map(lambda x: set(x), output)
		
	@staticmethod
	def createKeysFromProfileAttributes(attributes, keysToExclude = []):
		try:
			nonEmptyAttributes = filter(lambda a: not a.isEmpty(), attributes)
			values = filter(lambda x : len(x) > 0, map(lambda x : "" if x.key in keysToExclude else x.value.lower(), nonEmptyAttributes))
			split = map(lambda x: re.split('\W+', x), values)
			return list(set(filter(lambda x: len(x) > 0, [y.strip() for x in split for y in x])))
		except:
			return []