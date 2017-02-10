package BlockBuildingMethods

import DataStructures._
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 07/12/2016.
  *
  * Implements token blocking
  */
object TokenBlocking extends GenericBlocking {

  /**
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  override def createBlocks(profiles: RDD[Profile], separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).filter(_.trim.length>0).toList.distinct))
    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileID).groupByKey()

    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.toList

        val blockEntities = (entityIds.partition(_ <= separatorID))
        blockEntities
    }

    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorID < 0) block._2.size > 1
        else block._1.size * block._2.size >= 1
    } zipWithIndex()

    profilesGroupedWithIds map {
      c =>
        val blockId = c._2
        val entityIds = c._1
        if (separatorID < 0) BlockDirty(blockId, entityIds.swap)
        else BlockClean(blockId, entityIds)
    }
  }

  def createBlocksCluster(profiles : RDD[Profile], separatorID: Long, clusters : List[(Int, List[String], Double)], keysToExclude : Iterable[String] = Nil) : RDD[BlockAbstract] = {

    val defaultClusterID = clusters.filter(_._2.contains("tuttiTokenNonNeiCluster")).head._1
    val entropies = clusters.map(x => (x._1, x._3)).toMap
    val clusterMap = clusters.flatMap(c => c._2.map(w => (w, c._1))).toMap

    val tokensPerProfile = profiles.map{
      profile =>
        val dataset = if(profile.id > separatorID) "d_"+1+"_" else "d_"+2+"_"

        val tokens = profile.attributes.flatMap{
          keyValue =>
            if(keysToExclude.exists(_.equals(keyValue.key))) {
              Nil
            }
            else{
              val key = dataset + keyValue.key
              val values = keyValue.value.split("[\\W_]")
              val clusterID = clusterMap.getOrElse(key, defaultClusterID)
              values.map(_ + "_" + clusterID)
            }
        }.filter(_.size > 0)

        (profile.id, tokens.distinct)
    }

    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileID).groupByKey()

    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.toList
        val blockEntities = (entityIds.partition(_ <= separatorID))

        (c._1, blockEntities)
    }

    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorID < 0) block._2._2.size > 1
        else block._2._1.size * block._2._2.size >= 1
    } distinct() zipWithIndex()

    profilesGroupedWithIds map {
      c =>
        val blockId = c._2
        val originalID = c._1._1.split("_").last.toInt
        val entityIds = c._1._2
        if (separatorID < 0) BlockDirty(blockId, entityIds.swap, entropies(originalID))
        else BlockClean(blockId, entityIds, entropies(originalID))
    }
  }

  /**
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if is Dirty put -1
    * @return
    */
  def createBlocksWithEntropy(profiles: RDD[Profile], separatorID: Long, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).toList.distinct))
    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileID_entro).groupByKey()

    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.map(_._1).toList

        val allProfilesTokens = c._2.flatMap(_._2)

        val numberOfTokens = allProfilesTokens.size.toDouble

        val entropy = -allProfilesTokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens

        val blockEntities = (entityIds.partition(_ <= separatorID))
        (entropy, blockEntities)
    }

    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        val blocks = block._2
        if (separatorID < 0) blocks._2.size > 1
        else blocks._1.size * blocks._2.size >= 1
    } zipWithIndex()

    profilesGroupedWithIds map {
      c =>
        val blockId = c._2
        val entityIds = c._1._2
        val entropy = c._1._1
        if (separatorID < 0) BlockDirty(blockId, entityIds.swap, entropy)
        else BlockClean(blockId, entityIds, entropy)
    }
  }

  override def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude : Iterable[String]): Iterable[String] = {
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
        value.split("[\\W_]")
    }
  }
}