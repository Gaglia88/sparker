class Profile(object):
	"""
	Represents a profile.
	
	Attributes
	----------
	profileID : int
		internal profile identifier
	attributes : [KeyValue]
		profile data
	originalID : str
		profile identifier in the original data source
	sourceId : int
		source identifier
	"""
	def __init__(self, profileID, attributes = [], originalID = "", sourceId = 0):
		"""
		Parameters
		----------
		profileID : int
			internal profile identifier
		attributes : [KeyValue]
			profile data
		originalID : str
			profile identifier in the original data source
		sourceId : int
			source identifier
		"""
		self.profileID = profileID
		self.attributes = attributes
		self.originalID = originalID
		self.sourceId = sourceId
	
	def __str__(self):
		return str(self.__dict__)


class MatchingEntities(object):
	"""
	Represents two matching entities
	
	Attributes
	----------
	firstEntityID : str
		first profile id
	secondEntityID : str
		second profile id
	"""
	def __init__(self, firstEntityID, secondEntityID):
		self.firstEntityID = firstEntityID
		self.secondEntityID = secondEntityID
		

class KeyValue(object):
	"""
	Represents an attribute value with his key
	
	Attributes
	----------
	key : str
		attribute name
	value : str
		attribute' value
	"""
	def __init__(self, key, value):
		self.key = key
		self.value = value
	
	def isEmpty(self):
		"""
		Returns true if the value of the attribute is empty
		"""
		try:
			return self.key is None or self.value is None
		except:
			return True
	
	def __str__(self):
		return str(self.__dict__)


class BlockClean(object):
	"""
	Represents a clean block (i.e. the number of data sources is greater than 1)
	
	Attributes
	----------
	blockID : int
		block identifier
	profiles : [[int]]
		list of profiles contained in the block partitioned by their dataset
	entropy : float, optional
		block' entropy
	clusterID : int, optional
		id of the cluster from which the block derives
	blockingKey : string, optional
		blocking key who have generated the block
		
	"""
	def __init__(self, blockID, profiles, entropy = -1.0, clusterID = -1, blockingKey = ""):
		self.blockID = blockID
		self.profiles = profiles
		self.entropy = entropy
		self.clusterID = clusterID
		self.blockingKey = blockingKey
		
	def getSize(self):
		"""
		Returns the size of the blocks (i.e. number of profiles indexed in the block)
		"""
		return sum(map(lambda x: len(x), self.profiles))

	def getAllProfiles(self):
		"""
		Returns all the profiles contained in the block
		"""
		return [y for x in self.profiles for y in x]
	
	def getComparisonSize(self):
		"""
		Returns the number of comparisons involved by the block
		"""
		a = filter(lambda p: len(p) > 0, self.profiles)
		if(len(a) > 1):
			comparisons = 0
			i = 0
			while(i < len(self.profiles)):
				j = i + 1
				while (j < len(self.profiles)):
					comparisons += len(a[i]) * len(a[j])
					j += 1
				i+=1
			return comparisons
		else:
			return 0
	
	def __str__(self):
		return str(self.__dict__)


class BlockDirty(object):
	"""
	Represents a clean block (i.e. the number of data sources is greater than 1)
	
	Attributes
	----------
	blockID : int
		block identifier
	profiles : [[int]]
		list of profiles contained in the block partitioned by their dataset
	entropy : float, optional
		block' entropy
	clusterID : int, optional
		id of the cluster from which the block derives
	blockingKey : string, optional
		blocking key who have generated the block
	"""
	def __init__(self, blockID, profiles, entropy = -1, clusterID = -1, blockingKey = ""):
		self.blockID = blockID
		self.profiles = profiles
		self.entropy = entropy
		self.clusterID = clusterID
		self.blockingKey = blockingKey
		
	def getSize(self):
		"""
		Returns the size of the blocks (i.e. number of profiles indexed in the block)
		"""
		return sum(map(lambda x: len(x), self.profiles))

	def getAllProfiles(self):
		"""
		Returns all the profiles contained in the block
		"""
		return [y for x in self.profiles for y in x]
	
	def getComparisonSize(self):
		"""
		Returns the number of comparisons involved by the block
		"""
		return len(self.profiles[0])*(len(self.profiles[0])-1)
	
	def __str__(self):
		return str(self.__dict__)


class BlockWithComparisonSize(object):
	"""
	Represents a block with the number of comparisons involved by the block
	
	Attributes
	----------
	blockID : int
		block identifier
	comparisons
		number of comparisons involved by the block
	"""
	def __init__(self, blockID, comparisons):
		self.blockID = blockID
		self.comparisons = comparisons
	
	def __str__(self):
		return str(self.__dict__)


class ProfileBlocks(object):
	"""
	Represents a profile with the list of blocks in which it is contained
	
	Attributes
	----------
	profileID : int
		profile identifier
	blocks : [int]
		blocks in which the profile it is contained
	"""
	def __init__(self, profileID, blocks):
		self.profileID = profileID
		self.blocks = blocks
	
	def __str__(self):
		return "{profileID: "+str(self.profileID)+", blocks: "+str(self.blocks)+"}"


class KeysCluster(object):
	"""
	Represents a cluster of attributes.
	clusterID : int
		cluster identifier
	keys : [string]
		list of attributes contained in the cluster
	entropy : float, optional
		entropy of the cluster
	"""
	def __init__(self, clusterID, keys, entropy = 1.0):
		self.clusterID = clusterID
		self.keys = keys
		self.entropy = entropy
	
	def __str__(self):
		return str(self.__dict__)
