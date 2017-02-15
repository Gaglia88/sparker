package DataStructures

/**
  * Maps a profile with the blocks in which it is contained
  * @author Luca Gagliardelli
  * @since 2016/12/08
  */
case class ProfileBlocks(profileID : Long, blocks : List[BlockWithComparisonSize]) extends Serializable{}
