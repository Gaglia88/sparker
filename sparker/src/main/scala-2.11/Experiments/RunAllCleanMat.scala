import BlockRefinementMethods.PruningMethods.{PruningUtils, WNPFor}
import Experiments.{RunClean, RunCleanMat}
import org.apache.spark.{SparkConf, SparkContext}

/*
val pathDataset1 = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/citations/citeseer.csv"
val pathDataset2 = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/citations/dblp.csv"
val pathGt = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/citations/matches_citeseer_dblp.csv"
val defaultLogPath = "/marconi_scratch/userexternal/lgaglia1/log/"
val wrapperType = RunClean.WRAPPER_TYPES.csv
val GTWrapperType = RunClean.WRAPPER_TYPES.csv
val purgingRatio = 1.000
val filteringRatio = 0.8
val hashNum = 16
val clusterThreshold = 0.3
val memoryHeap = 118
val memoryStack = 5

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

val saveResPath = "/marconi_scratch/userexternal/lgaglia1/ER/"
RunCleanMat.RunCleanMat(sc, defaultLogPath+"WNP_CBS_AVG_NO-SCHEMA.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaAware, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, saveResPath)

RunClean.runClean(sc, defaultLogPath+"WNP_CBS_AVG_NO-SCHEMA.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaAware, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)
RunClean.runClean(sc, defaultLogPath+"WNP_CBS_AVG_SCHEMA_NO-ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)
RunClean.runClean(sc, defaultLogPath+"WNP_CBS_AVG_SCHEMA_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = true, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)

RunClean.runClean(sc, defaultLogPath+"WNP_CBS_MAXDiv2_NO-SCHEMA.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaAware, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.MAX_FRACT_2, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)
RunClean.runClean(sc, defaultLogPath+"WNP_CBS_MAXDiv2_SCHEMA_NO-ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.MAX_FRACT_2, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)
RunClean.runClean(sc, defaultLogPath+"WNP_CBS_MAXDiv2_SCHEMA_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.inferSchema, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.MAX_FRACT_2, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = true, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType)
*/