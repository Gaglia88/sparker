package Experiments

import java.io.{IOException, _}

import DataStructures.{KeyValue, Profile}

/**
  * Created by Luca on 02/02/2017.
  */
object TestSaveObj {
  def main(args: Array[String]) {

    println("Iniziato caricamento")
    val entities = DataLoaders.SerializedLoader.loadSerializedDataset(args(0))
    println("Terminato caricamento")
    var profiles : Array[Profile] = new Array(entities.size())

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
    println("Terminata conversione")

    storeSerializedObject(profiles, args(1))
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
