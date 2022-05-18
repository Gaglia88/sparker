class Profile(object):
	"""
	Represents a profile.
	
	Attributes
	----------
	profile_id : int
		internal profile identifier
	attributes : [Key_value]
		profile data
	original_id : str
		profile identifier in the original data source
	source_id : int
		source identifier
	"""
	def __init__(self, profile_id, attributes=None, original_id="", source_id=0):
		"""
		Parameters
		----------
		profile_id : int
			internal profile identifier
		attributes : [Key_value]
			profile data
		original_id : str
			profile identifier in the original data source
		source_id : int
			source identifier
		"""
		if attributes is None:
			attributes = []
		self.profile_id = profile_id
		self.attributes = attributes
		self.original_id = original_id
		self.source_id = source_id

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class MatchingEntities(object):
	"""
	Represents two matching entities
	
	Attributes
	----------
	first_entity_id : str
		first profile id
	second_entity_id : str
		second profile id
	"""
	def __init__(self, first_entity_id, second_entity_id):
		self.first_entity_id = first_entity_id
		self.second_entity_id = second_entity_id
		

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
	
	def is_empty(self):
		"""
		Returns true if the value of the attribute is empty
		"""
		try:
			return self.key is None or self.value is None
		except Exception:
			return True

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class BlockClean(object):
	"""
	Represents a clean block (i.e. the number of data sources is greater than 1)
	
	Attributes
	----------
	block_id : int
		block identifier
	profiles : [[int]]
		list of profiles contained in the block partitioned by their dataset
	entropy : float, optional
		block' entropy
	cluster_id : int, optional
		id of the cluster from which the block derives
	blocking_key : string, optional
		blocking key who have generated the block
		
	"""
	def __init__(self, block_id, profiles, entropy=-1.0, cluster_id=-1, blocking_key=""):
		self.block_id = block_id
		self.profiles = profiles
		self.entropy = entropy
		self.cluster_id = cluster_id
		self.blocking_key = blocking_key
		
	def get_size(self):
		"""
		Returns the size of the blocks (i.e. number of profiles indexed in the block)
		"""
		return sum(map(lambda x: len(x), self.profiles))

	def get_all_profiles(self):
		"""
		Returns all the profiles contained in the block
		"""
		return [y for x in self.profiles for y in x]
	
	def get_comparison_size(self):
		"""
		Returns the number of comparisons involved by the block
		"""
		a = list(filter(lambda p: len(p) > 0, self.profiles))
		if len(a) > 1:
			comparisons = 0
			i = 0
			while i < len(a):
				j = i + 1
				while j < len(a):
					comparisons += len(a[i]) * len(a[j])
					j += 1
				i += 1
			return comparisons
		else:
			return 0

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class BlockDirty(object):
	"""
	Represents a clean block (i.e. the number of data sources is greater than 1)
	
	Attributes
	----------
	block_id : int
		block identifier
	profiles : [[int]]
		list of profiles contained in the block partitioned by their dataset
	entropy : float, optional
		block' entropy
	cluster_id : int, optional
		id of the cluster from which the block derives
	blocking_key : string, optional
		blocking key who have generated the block
	"""
	def __init__(self, block_id, profiles, entropy=-1, cluster_id=-1, blocking_key=""):
		self.block_id = block_id
		self.profiles = profiles
		self.entropy = entropy
		self.cluster_id = cluster_id
		self.blocking_key = blocking_key
		
	def get_size(self):
		"""
		Returns the size of the blocks (i.e. number of profiles indexed in the block)
		"""
		return sum(map(lambda x: len(x), self.profiles))

	def get_all_profiles(self):
		"""
		Returns all the profiles contained in the block
		"""
		return [y for x in self.profiles for y in x]
	
	def get_comparison_size(self):
		"""
		Returns the number of comparisons involved by the block
		"""
		return len(self.profiles[0])*(len(self.profiles[0])-1)

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class BlockWithComparisonSize(object):
	"""
	Represents a block with the number of comparisons involved by the block
	
	Attributes
	----------
	block_id : int
		block identifier
	comparisons
		number of comparisons involved by the block
	"""
	def __init__(self, block_id, comparisons):
		self.block_id = block_id
		self.comparisons = comparisons

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class ProfileBlocks(object):
	"""
	Represents a profile with the list of blocks in which it is contained
	
	Attributes
	----------
	profile_id : int
		profile identifier
	blocks : [int]
		blocks in which the profile it is contained
	"""
	def __init__(self, profile_id, blocks):
		self.profile_id = profile_id
		self.blocks = blocks

	def __repr__(self):
		return "{profile_id: "+str(self.profile_id)+", blocks: "+str(self.blocks)+"}"

	def __str__(self):
		return self.__repr__()


class KeysCluster(object):
	"""
	Represents a cluster of attributes.
	cluster_id : int
		cluster identifier
	keys : [string]
		list of attributes contained in the cluster
	entropy : float, optional
		entropy of the cluster
	"""
	def __init__(self, cluster_id, keys, entropy = 1.0):
		self.cluster_id = cluster_id
		self.keys = keys
		self.entropy = entropy

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()


class Attr(object):
	"""
		Represents an attribute with the source from which belongs
		Attributes
		----------
		source_name : int
			identifier of the source from which the attribute belongs
		attribute : str
			name of the attribute
	"""

	def __init__(self, source_name, attribute):
		self.source_name = source_name
		self.attribute = attribute

	def __repr__(self):
		return str(self.__dict__)

	def __str__(self):
		return self.__repr__()

	def __eq__(self, other):
		return self.source_name == other.source_name and self.attribute == other.attribute

	def __hash__(self):
		return hash((self.source_name, self.attribute))