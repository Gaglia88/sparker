package BlockBuildingMethods

import BlockBuildingMethods.LSHMio.Settings
//import BlockBuildingMethods.LSHTwitter.Settings
import DataStructures.{BlockAbstract, KeyValue, Profile}
import org.apache.spark.rdd.RDD

/**
 * Common methods for the different blocking techniques
 * @author Luca Gagliardelli
 * @since 2016/12/07
 */
object BlockingUtils {
  /** Defines the pattern used for tokenization */
  object TokenizerPattern {
    /** Split the token by underscore, whitespaces and punctuation */
    val DEFAULT_SPLITTING = "[\\W_]"
  }

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
