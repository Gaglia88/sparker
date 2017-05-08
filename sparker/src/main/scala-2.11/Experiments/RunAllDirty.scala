import BlockBuildingMethods.LSHTwitter.Settings
import BlockRefinementMethods.PruningMethods.{PruningUtils, WNPFor}
import DataStructures.KeysCluster
import Experiments.{RunClean, RunDirty}
import org.apache.spark.{SparkConf, SparkContext}
/*
val pathDataset1 = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/songs/msd.csv"
val pathGt = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/songs/matches_msd_msd.csv"
val defaultLogPath = "/marconi_scratch/userexternal/lgaglia1/log/"
val wrapperType = RunClean.WRAPPER_TYPES.csv
val GTWrapperType = RunClean.WRAPPER_TYPES.csv
val purgingRatio = 1.015
val filteringRatio = 0.8
val hashNum = 16
val clusterThreshold = 0.3
val memoryHeap = 14
val memoryStack = 1
val realId = "id"

val conf = new SparkConf()
  .setAppName("Main")
  .setMaster("local[*]")
  .set("spark.executor.memory", memoryHeap+"g")
  .set("spark.network.timeout", "10000s")
  .set("spark.executor.heartbeatInterval", "40s")
  .set("spark.default.parallelism", "32")
  .set("spark.executor.extraJavaOptions", "-Xss"+memoryStack+"g")
  .set("spark.local.dir", "/data2/sparkTmp/")
  .set("spark.driver.maxResultSize", "10g")

val sc = new SparkContext(conf)

RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_NO_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaAware, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId)
//RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId)
//RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_SCHEMA_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = true, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId)

/*
val attributes = "id,title,release,artist_name,duration,artist_familiarity,artist_hotttnesss,year".split(",").toList
RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_MANUAL_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.manualMapping, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId, attributes)
RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_MANUAL_SCHEMA_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.manualMapping, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = true, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId, attributes)
*/
*/