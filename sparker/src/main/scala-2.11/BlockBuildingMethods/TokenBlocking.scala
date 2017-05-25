package BlockBuildingMethods

import DataStructures._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Implements the token blocking
  *
  * @author Luca Gagliardelli
  * @since 2016/12/07
  */
object TokenBlocking {

  /**
    * Performs the token blocking
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocks(profiles: RDD[Profile], separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).filter(_.trim.length>0).toList.distinct))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.toSet

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
    * Performs a schema aware tokenblocking
    *
    * @param profiles input to profiles to create blocks
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToUse keys to use in the blocking process
    * @return the blocks
    */
  def createBlocksUsingSchema(profiles: RDD[Profile], separatorID: Long = -1, keysToUse : List[String] = Nil): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map{
      profile =>

        val tokens = profile.attributes.flatMap{
          keyValue =>
            if(keysToUse.indexOf(keyValue.key) >= 0) {
              val key = keyValue.key
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size>0).map(_.toLowerCase)
              values.map(key+"_"+_)
            }
            else{
              Nil
            }
        }.filter(_.size > 0)

        (profile.id, tokens)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.toSet

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

  def createBlocksUsingSchemaEntropy(profiles: RDD[Profile], separatorID: Long = -1, keysToUse : List[String] = Nil): RDD[BlockAbstract] = {
    val tokensPerProfile2 = profiles.map{
      profile =>
        val tokens = profile.attributes.map{
          keyValue =>
            if(keysToUse.indexOf(keyValue.key) >= 0) {
              val key = keyValue.key
              val tokens = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size>0).map(_.toLowerCase)
              val a : Iterable[(String, String)] = tokens.map(t => (key, t))
              a
            }
            else{
              Nil
            }
        }.filter(_.size > 0)
        tokens.flatten
    }

    val sc = SparkContext.getOrCreate()

    val entropyMap = sc.broadcast(tokensPerProfile2.flatMap(x => x).groupByKey().map{
      case(key, tokens) =>
        val numberOfTokens = tokens.size.toDouble
        val entropy = -tokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens
        (key, entropy)
    }.collectAsMap())

    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map{
      profile =>
        val tokens = profile.attributes.flatMap{
          keyValue =>
            if(keysToUse.indexOf(keyValue.key) >= 0) {
              val key = keyValue.key
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size>0).map(_.toLowerCase)
              values.map(key+"_!!_"+_)
            }
            else{
              Nil
            }
        }.filter(_.size > 0)

        (profile.id, tokens)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()

    val profilesWithEntropy = profilePerKey.map{
      case(key, profiles) =>
        val entropy = entropyMap.value.get(key.split("_!!_").head).get
        (entropy, profiles)
    }

    entropyMap.unpersist()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilesWithEntropy map {
      case(entropy, entityIds) =>
        val blockEntities = (entityIds.partition(_ <= separatorID))
        (entropy, blockEntities)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case(entropy, block) =>
        if (separatorID < 0) block._2.size > 1
        else block._1.size * block._2.size >= 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      x =>
        val blockId = x._2
        val entityIds = x._1._2
        val entropy = x._1._1
        if (separatorID < 0) BlockDirty(blockId, (entityIds._2.toSet, entityIds._1.toSet), entropy)
        else BlockClean(blockId, (entityIds._1.toSet, entityIds._2.toSet), entropy)
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
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile = profiles.map{
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = if(profile.id > separatorID) LSHTwitter.Settings.FIRST_DATASET_PREFIX else LSHTwitter.Settings.SECOND_DATASET_PREFIX

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap{
          keyValue =>
            if(keysToExclude.exists(_.equals(keyValue.key))) {//If this key is in the exclusion list ignores this tokens
              Nil
            }
            else{
              val key = dataset + keyValue.key  //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.size>0).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID) //Add the cluster id to the tokens
            }
        }.filter(_.size > 0)

        (profile.id, tokens.distinct)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      case(attribute, entityIds) =>
        val blockEntities = (entityIds.partition(_ <= separatorID))
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = attribute.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          catch{
            case _ : Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID) =>
        if (separatorID < 0) block._2.size > 1
        else block._1.size * block._2.size >= 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case((entityIds, entropy, clusterID), blockId) =>
        if (separatorID < 0) BlockDirty(blockId, (entityIds._2.toSet, entityIds._1.toSet), entropy, clusterID)
        else BlockClean(blockId, (entityIds._1.toSet, entityIds._2.toSet), entropy, clusterID)
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
    val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileIdEntropy).groupByKey()

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean)
     * and also calculates the entropy */
    val profilesGrouped = profilePerKey map {
      c =>
        val entityIds = c._2.map(_._1).toSet       //Id of the entity
        val allProfilesTokens = c._2.flatMap(_._2)  //All tokens contained in the entity
        val numberOfTokens = allProfilesTokens.size.toDouble
        val entropy = -allProfilesTokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens
        val blockEntities = (entityIds.partition(_ <= separatorID))
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
    *
    * @param attributes attributes to tokenize
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    * */
  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude : Iterable[String]): Iterable[String] = {
    attributes.map{
      at =>
        if(keysToExclude.exists(_.equals(at.key))){
          ""
        }
        else{
          at.value.toLowerCase
        }
    } filter(_.trim.length > 0) flatMap {
      value =>
        value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    }
  }
}