package BlockBuildingMethods

import DataStructures._
import org.apache.spark.rdd.RDD

/**
  * Implements the token blocking
  *
  * @author Luca Gagliardelli
  * @since 2016/12/07
  */
object TokenBlocking extends GenericBlocking {

  /**
    * Performs the token blocking
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  override def createBlocks(profiles: RDD[Profile], separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).filter(_.trim.length>0).toList.distinct))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileID).groupByKey()

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

  /**
    * Performs the token blocking clustering the attributes by the keys.
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param clusters
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocksCluster(profiles : RDD[Profile], separatorID: Long, clusters : List[KeysCluster], keysToExclude : Iterable[String] = Nil) : RDD[BlockAbstract] = {
    /** Obtains the ID of the default cluster: all the elements that are not in other clusters finish in this one */
    val defaultClusterID = clusters.filter(_.keys.contains(LSHTwitter.Settings.DEFAULT_CLUSTER_NAME)).head.id
    /** Creates a map that contains the entropy for each cluster */
    val entropies = clusters.map(x => (x.id, x.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile = profiles.map{
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = if(profile.id > separatorID) "d_"+1+"_" else "d_"+2+"_"

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap{
          keyValue =>
            if(keysToExclude.exists(_.equals(keyValue.key))) {//If this key is in the exclusion list ignores this tokens
              Nil
            }
            else{
              val key = dataset + keyValue.key  //Add the dataset suffix to the key
              val values = keyValue.value.split(TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size>0).distinct //Split the values and obtain the tokens
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              values.map(_ + "_" + clusterID) //Add the cluster id to the tokens
            }
        }.filter(_.size > 0)

        (profile.id, tokens.distinct)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileID).groupByKey()

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

  /**
    * Performs the token blocking and calculates the entropy of each block
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocksWithEntropy(profiles: RDD[Profile], separatorID: Long, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).filter(_.trim.length>0)))
    /* Associate each profile to each token, produces (tokenID, [(profileID, [profile tokens hashes])) */
    val profilePerKey = tokensPerProfile.flatMap(associateKeysToProfileIdEntropy).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean)
     * and also calculates the entropy */
    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.map(_._1).toList       //Id of the entity
        val allProfilesTokens = c._2.flatMap(_._2)  //All tokens contained in the entity
        val numberOfTokens = allProfilesTokens.size.toDouble
        val entropy = -allProfilesTokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens
        val blockEntities = (entityIds.distinct.partition(_ <= separatorID))
        (entropy, blockEntities)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        val entityIds = block._2
        if (separatorID < 0) entityIds._2.size > 1
        else entityIds._1.size * entityIds._2.size >= 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case ((entropy, entityIds), blockId) =>
        if (separatorID < 0) BlockDirty(blockId, entityIds.swap, entropy)
        else BlockClean(blockId, entityIds, entropy)
    }
  }

  /**
    * Given a list of key-value items returns the tokens.
    * @param attributes attributes to tokenize
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    * */
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
        value.split(TokenizerPattern.DEFAULT_SPLITTING)
    }
  }
}