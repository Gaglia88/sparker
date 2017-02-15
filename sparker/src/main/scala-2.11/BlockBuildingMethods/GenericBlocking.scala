package BlockBuildingMethods

import DataStructures.{KeyValue, BlockAbstract, Profile}
import org.apache.spark.rdd.RDD

/**
 * Generic class to extends to create a block method
 * @author Luca Gagliardelli
 * @since 2016/12/07
 */
trait GenericBlocking {
  /** Defines the pattern used for tokenization */
  object TokenizerPattern {
    /** Split the token by underscore, whitespaces and punctuation */
    val DEFAULT_SPLITTING = "[\\W_]"
  }

  /**
   * Method to create blocks.
   *
   * @param profiles input profiles to create blocks
   * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
   **/
  def createBlocks(profiles: RDD[Profile], separatorID: Long, keysToExclude : Iterable[String]): RDD[BlockAbstract]

  /**
   * Method used to extract tokens from the profile' attributes
    *
    * @param attributes attributes list
   **/
  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude : Iterable[String]): Iterable[String]


  /**
   * Given a tuple (entity ID, [List of entity tokens])
   * produces a list of tuple (token, entityID)
    *
    * @param profileKs couple (entity ID, [List of entity keys])
   **/
  def associateKeysToProfileID(profileKs: (Long, Iterable[String])): Iterable[(String, Long)] = {
    val profileId = profileKs._1
    val keys = profileKs._2
    keys.map(key => (key, profileId))
  }

  /**
    * Used in the method that calculates the entropy of each block
    * @param profileKs couple (entity ID, [List of entity' tokens])
    * @return a list of (token, (profileID, [tokens hashes]))
    **/
  def associateKeysToProfileIdEntropy(profileKs: (Long, Iterable[String])): Iterable[(String, (Long, Iterable[Int]))] = {
    val profileId = profileKs._1
    val tokens = profileKs._2
    tokens.map(tokens => (tokens, (profileId, tokens.map(_.hashCode))))
  }
}
