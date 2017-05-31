package Utilities

import DataStructures._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

/**
  * Created by Luca on 31/05/2017.
  */
class SparkErRegistrator extends KryoRegistrator{

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BlockClean])
    kryo.register(classOf[BlockDirty])
    kryo.register(classOf[BlockWithComparisonSize])
    kryo.register(classOf[KeysCluster])
    kryo.register(classOf[KeyValue])
    kryo.register(classOf[MatchingEntities])
    kryo.register(classOf[Profile])
    kryo.register(classOf[Array[Profile]])
    kryo.register(classOf[ProfileBlocks])
    kryo.register(classOf[UnweightedEdge])
    kryo.register(classOf[WeightedEdge])

    kryo.register(DataStructures.Profile.getClass)
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
  }
}