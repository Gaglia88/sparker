package DataStructures

/**
  * Created by Luca on 12/12/2016.
  *
  * Represents a block identified by an ID with its comparison level (number of comparison in the block)
  */
case class BlockWithComparisonSize(blockID : Long, comparisons : Double) extends Ordered[BlockWithComparisonSize]{
  def compare(that : BlockWithComparisonSize) : Int = {
    that.comparisons compare this.comparisons
  }
}
