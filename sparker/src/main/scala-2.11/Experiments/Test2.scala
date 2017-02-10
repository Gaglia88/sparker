package Experiments

import java.io.{ObjectOutputStream, _}

import DataStructures.{KeyValue, Profile}

/**
  * Created by Luca on 03/02/2017.
  */
object Test2 {
  def main(args: Array[String]) {

    /*println(loadSerializedObject("C:\\\\Users\\\\Luca\\\\Desktop\\\\datasets\\\\dataset2_7").asInstanceOf[Array[Profile]].last.id)

    val a = new Array[Array[Profile]](8)

    for(i <- 0 to 7){
      a.update(i, loadSerializedObject("C:\\\\Users\\\\Luca\\\\Desktop\\\\datasets\\\\dataset2_"+i).asInstanceOf[Array[Profile]])
    }

    storeSerializedObject(a.flatten, "C:\\Users\\Luca\\Desktop\\datasets2\\dataset2_id_start_last_dataset1")*/

    /*


    println("Iniziato caricamento")
    val entities = DataLoaders.SerializedLoader.loadSerializedDataset("/data2/er/data/dbpedia/profiles/dataset2")
    println("Terminato caricamento")

    println("Numero elementi "+entities.size())


    var chunkSize = 270505
    //while(entities.size()%chunkSize != 0) chunkSize-=1;

    println("Chunk size "+chunkSize)

    var chunk = 0

    var profiles : Array[Profile] = new Array(chunkSize)

    for(i <- 0 to entities.size()-1){
      val profile = Profile(i)

      profile.addAttribute(KeyValue("id", (i+1190733)+""))

      val entity = entities.get(i)
      val it = entity.getAttributes.iterator()
      while(it.hasNext){
        val attribute = it.next()
        profile.addAttribute(KeyValue(attribute.getName, attribute.getValue))
      }

      profiles.update(i%chunkSize, profile)

      if((i+1)%chunkSize == 0){
        storeSerializedObject(profiles, "/data2/er/dataset2_"+chunk)
        chunk+=1
      }
    }
    println("Terminata conversione")*/

    println("Iniziato caricamento")
    val entities = DataLoaders.SerializedLoader.loadSerializedDataset("/data2/er/data/dbpedia/profiles/dataset1")
    println("Terminato caricamento")

    println("Numero elementi "+entities.size())

    val profiles : Array[Profile] = new Array(entities.size())

    for(i <- 0 to entities.size()-1){
      val profile = Profile(i)

      profile.addAttribute(KeyValue("id", i+""))

      val entity = entities.get(i)
      val it = entity.getAttributes.iterator()
      while(it.hasNext){
        val attribute = it.next()
        profile.addAttribute(KeyValue(attribute.getName, attribute.getValue))
      }

      profiles.update(i, profile)
    }

    storeSerializedObject(profiles, "/data2/er/dataset1")
    println("Terminata conversione")

  }

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

  def storeSerializedObject(`object`: Any, outputPath: String) {
    try {
      val file: OutputStream = new FileOutputStream(outputPath)
      val buffer: OutputStream = new BufferedOutputStream(file)
      val output: ObjectOutput = new ObjectOutputStream(buffer)
      try {
        output.writeObject(`object`)
      } finally {
        output.close
      }
    }
    catch {
      case ioex: IOException => {
        ioex.printStackTrace
      }
    }
  }
}
