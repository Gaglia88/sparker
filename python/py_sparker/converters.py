from objects import Profile, MatchingEntities, KeyValue, BlockClean, BlockDirty, BlockWithComparisonSize, ProfileBlocks
from blockers import TokenBlocking

class Converters(object):
	"""
		Contains converters to convert RDDs to different object types
	"""
	@staticmethod
	def blockIDProfileIDFromBlock(block):
		"""
		Given a block (clean or dirty), produces a list of (p_i, BlockWithComparisonSize)
		where p_i is a profile indexed in the block.
		
		Parameters
		----------
		block : BlockClean|BlockDirty
			A block (clean or dirty)
		"""
		blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize())
		return map(lambda p: (p, blockWithComparisonSize), block.getAllProfiles())

	@staticmethod
	def blocksToProfileBlocks(blocks):
		"""
		Given an RDD of blocks (clean or dirty), produces an RDD of ProfileBlocks.
		
		Parameters
		----------
		block : RDD[BlockClean|BlockDirty]
			An RDD of blocks (clean or dirty)
		"""
		profilesPerBlocks = blocks.flatMap(Converters.blockIDProfileIDFromBlock).groupByKey()
		return profilesPerBlocks.map(lambda b : ProfileBlocks(b[0], set(b[1])))

	@staticmethod	
	def profilesBlockToBlocks(profilesBlocks, separatorIDs = []):
		"""
		Given an RDD of ProfileBlocks, produces an RDD of blocks (clean or dirty).
		
		Parameters
		----------
		profilesBlocks : RDD[ProfileBlocks]
			An RDD of profile blocks
		separatorIDs : [int]
			Id of separators (if the ER is clean) that identifies how to split profiles across the different sources
		"""
		def getBlock(data):
			blockID = data[0]
			profilesIDs = set(data[1])
			if len(separatorIDs) == 0:
				return BlockDirty(blockID, [profilesIDs])
			else:
				return BlockClean(blockID, TokenBlocking.separateProfiles(profilesIDs, separatorIDs))
				
		blockIDProfileID = profilesBlocks.flatMap(lambda pb: [(b.blockID, pb.profileID) for b in pb.blocks])
		blocks = blockIDProfileID.groupByKey().map(getBlock)
		return blocks.filter(lambda b: b.getComparisonSize() > 0)