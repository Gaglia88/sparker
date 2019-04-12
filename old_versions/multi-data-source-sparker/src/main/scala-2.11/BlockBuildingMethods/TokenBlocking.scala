package BlockBuildingMethods

import BlockBuildingMethods.LSHMio.Settings
import DataStructures._
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD

/**
  * Implements the token blocking
  *
  * @author Luca Gagliardelli
  * @since 2016/12/07
  */
object TokenBlocking {

  def removeBadWords(input: RDD[(String, Long)]): RDD[(String, Long)] = {
    val sc = SparkContext.getOrCreate()
    val stopwords = sc.broadcast(StopWordsRemover.loadDefaultStopWords("english"))
    input
      .filter(x => x._1.matches("[A-Za-z]+") || x._1.matches("[0-9]+"))
      .filter(x => !stopwords.value.contains(x._1))
  }


  /**
    * Performs the token blocking
    *
    * @param profiles      input to profiles to create blocks
    * @param separatorIDs  id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocks(profiles: RDD[Profile], separatorIDs: Array[Long] = Array.emptyLongArray, keysToExclude: Iterable[String] = Nil, removeStopWords: Boolean = false): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFromProfileAttributes(profile.attributes, keysToExclude).filter(_.trim.length > 0).toList.distinct))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    //val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(_._2.size > 1)
    val a = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }
    val profilePerKey = a.groupByKey().filter(_._2.size > 1)

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey.map {
      c =>
        val entityIds = c._2.toSet

        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds)
          }
          else {
            TokenBlocking.separateProfiles(entityIds, separatorIDs)
          }
        }
        blockEntities
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1

    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case (entityIds, blockId) =>
        if (separatorIDs.isEmpty)
          BlockDirty(blockId, entityIds)
        else BlockClean(blockId, entityIds)
    }
  }

  def createBlocksClusterDebug(profiles: RDD[Profile], separatorIDs: Array[Long], clusters: List[KeysCluster], keysToExclude: Iterable[String] = Nil, excludeDefaultCluster: Boolean = false, clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR):

  (RDD[BlockAbstract], scala.collection.Map[String, Map[Long, Iterable[String]]]) = {
    /** Obtains the ID of the default cluster: all the elements that are not in other clusters finish in this one */
    val defaultClusterID = clusters.maxBy(_.id).id
    /** Creates a map that contains the entropy for each cluster */
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile1 = profiles.map {
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              //If this key is in the exclusion list ignores this tokens
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID).map(x => (x, key)) //Add the cluster id to the tokens
            }
        }.filter(x => x._1.nonEmpty)

        (profile.id, tokens)
    }

    val debug = tokensPerProfile1.flatMap { case (profileId, tokens) =>
      tokens.map { case (token, attribute) =>
        (token, (profileId, attribute))
      }
    }.groupByKey().map(x => (x._1, x._2.groupBy(_._1).map(y => (y._1, y._2.map(_._2))))).collectAsMap()

    val tokensPerProfile = tokensPerProfile1.map(x => (x._1, x._2.map(_._1).distinct))

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    val blocks : RDD[BlockAbstract] = profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) BlockDirty(blockId, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else BlockClean(blockId, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }

    (blocks, debug)
  }

  /**
    * Performs the token blocking clustering the attributes by the keys.
    *
    * @param profiles      input to profiles to create blocks
    * @param separatorIDs  id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param clusters
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocksCluster(profiles: RDD[Profile], separatorIDs: Array[Long], clusters: List[KeysCluster], keysToExclude: Iterable[String] = Nil, excludeDefaultCluster: Boolean = false, clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR): RDD[BlockAbstract] = {
    /** Obtains the ID of the default cluster: all the elements that are not in other clusters finish in this one */
    val defaultClusterID = clusters.maxBy(_.id).id
    /** Creates a map that contains the entropy for each cluster */
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile = profiles.map {
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              //If this key is in the exclusion list ignores this tokens
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID) //Add the cluster id to the tokens
            }
        }.filter(_.nonEmpty)

        (profile.id, tokens.distinct)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) BlockDirty(blockId, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else BlockClean(blockId, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }
  }

  /**
    * Given a list of key-value items returns the tokens.
    *
    * @param attributes    attributes to tokenize
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    **/
  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String]): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ""
        }
        else {
          at.value.toLowerCase
        }
    } filter (_.trim.length > 0) flatMap {
      value =>
        value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    }
  }

  def separateProfiles(elements: Set[Long], separators: Array[Long]): Array[Set[Long]] = {
    var input = elements
    var output: List[Set[Long]] = Nil
    separators.foreach { sep =>
      val a = input.partition(_ <= sep)
      input = a._2
      output = a._1 :: output
    }
    output = input :: output

    output.reverse.toArray
  }
}