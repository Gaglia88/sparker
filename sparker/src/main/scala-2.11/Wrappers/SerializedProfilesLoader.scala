package Wrappers

import java.io.{IOException, _}

import DataStructures.Profile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 04/02/2017.
  * Classe per ricaricare un array di profili salvato in un file serializzato
  */
object SerializedProfilesLoader {

  /**
    * Carica i profili contenuti in un file serializzato.
    * @param filePath percorso file serializzato da caricare
    * @param chunkSize siccome il file può essere molto grosso viene parallelizzato in RDD a pezzi e poi unito, questa è la dimensione di un pezzo
    * @param startIDFrom serve se si vogliono aumentare gli ID dei profili di un certo valore, secondo me è costoso, è meglio averli già salvati con gli id aumentati
    * */
  def loadProfiles(filePath : String, chunkSize : Int = 10000, startIDFrom : Long = -1) : RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val data = loadSerializedObject(filePath).asInstanceOf[Array[Profile]]
    val profiles = sc.union(data.grouped(chunkSize).map(sc.parallelize(_)).toArray)

    if(startIDFrom > 0){
      profiles.map(p => Profile(p.id+startIDFrom, p.attributes))
    }
    else{
      profiles
    }
  }

  /**
    * Carica un oggetto serializzato
    * */
  def loadSerializedObject(fileName: String): Any = {
    var `object`: Any = null
    try {
      val file: InputStream = new FileInputStream(fileName)
      val buffer: InputStream = new BufferedInputStream(file)
      val input: ObjectInput = new ObjectInputStream(buffer)
      try {
        `object` = input.readObject
      } finally {
        input.close
      }
    }
    catch {
      case cnfEx: ClassNotFoundException => {
        System.err.println(fileName)
        cnfEx.printStackTrace
      }
      case ioex: IOException => {
        System.err.println(fileName)
        ioex.printStackTrace
      }
    }
    return `object`
  }
}
