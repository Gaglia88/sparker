package DataStructures

/**
  * Represents a cluster of keys.
  * Meanings keys that should represents the same thing.
  * E.g. "name" and "nome" are the same thing in two different languages.
  *
  * @author Luca Gagliardelli
  * @since 14/02/2017.
  */
case class KeysCluster(id : Int, keys: List[String], entropy : Double = -1){}
