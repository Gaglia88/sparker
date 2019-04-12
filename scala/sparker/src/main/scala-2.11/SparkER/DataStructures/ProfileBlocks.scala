package SparkER.DataStructures

/**
  * Maps a profile with the blocks in which it is contained.
  * For each block is mantained its size.
  * @author Luca Gagliardelli
  * @since 2016/12/08
  */
case class ProfileBlocks(profileID : Long, blocks : Set[BlockWithComparisonSize]) extends Serializable{}
