package BlockBuildingMethods

import DataStructures._
import org.apache.spark.rdd.RDD

/**
  * Implements the blocking using N-Grams
  *
  * @author Luca Gagliardelli
  * @since 2017/02/22
  */
object NGrams {

  /**
    * Performs the N-Grams blocking
    *
    * @param profiles input to profiles to create blocks
    * @param nGramSize ngrams length
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocks(profiles: RDD[Profile], nGramSize : Int, separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes2(profile.attributes, nGramSize, keysToExclude).filter(_.trim.length>0).toList.distinct))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.toList

        val blockEntities = (entityIds.partition(_ <= separatorID))
        blockEntities
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorID < 0) block._2.size > 1
        else block._1.size * block._2.size >= 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case(entityIds, blockId) =>
        if (separatorID < 0) BlockDirty(blockId, entityIds.swap)
        else BlockClean(blockId, entityIds)
    }
  }

  def createKeysFromProfileAttributes2(attributes: Iterable[KeyValue], ngramSize : Int, keysToExclude : Iterable[String]): Iterable[String] = {
    val stuff = List.fill(ngramSize-1){"$"}.mkString("")
    var x = 0
    attributes.map{
      at =>
        if(keysToExclude.exists(_.equals(at.key))){
          ""
        }
        else{
          at.value
        }
    } filter(_.trim.length > 0) flatMap {
      value =>
        //value.split("\\W+")
        value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    } filter(_.length > 0) flatMap {
      value =>
        val res = (stuff+value+stuff).toLowerCase.sliding(ngramSize).map(_.mkString).map{
          str =>
            x+=1
            str+"_"+x
        }
        x = 0
        res
    }
  }

  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], ngramSize : Int, keysToExclude : Iterable[String]): Iterable[String] = {
    val stuff = List.fill(ngramSize-1){"$"}.mkString("")
    attributes.map{
      at =>
        if(keysToExclude.exists(_.equals(at.key))){
          ""
        }
        else{
          at.value.trim.replace(" ", "_")
        }
    } filter(_.length > 0) flatMap {
      value =>
        (stuff+value+stuff).toLowerCase.sliding(ngramSize).map(_.mkString)
    }
  }
}
