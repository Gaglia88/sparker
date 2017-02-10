package BlockBuildingMethods

import DataStructures.{KeyValue, BlockAbstract, Profile}
import org.apache.spark.rdd.RDD

/**
 * Created by Luca on 07/12/2016.
 */
trait GenericBlocking {
  /**
   * Method to create blocks.
    *
    * @param profiles input to profiles to create blocks
   * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if is Dirty put -1
   **/
  def createBlocks(profiles: RDD[Profile], separatorID: Long, keysToExclude : Iterable[String]): RDD[BlockAbstract]

  /**
   * Method to extract tokens from the profile attributes
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

  def associateKeysToProfileID_entro(profileKs: (Long, Iterable[String])): Iterable[(String, (Long, Iterable[Int]))] = {
    val profileId = profileKs._1
    val keys = profileKs._2
    keys.map(key => (key, (profileId, test(keys))))
  }

  def test(tokens: Iterable[String]): Iterable[Int] = {
    tokens.map(_.hashCode)
  }
}
