package SparkSQL.test

object utils {

//  @param r
//  @param chunks
//  @return

  def splitRange(r:Range,chunks:Int):Seq[Range]={
    if (r.step !=1)
      throw new IllegalArgumentException("Range must have step size equal to 1")
    val nchunks = scala.math.max(chunks,1)
    val chunkSize = scala.math.max(r.length/nchunks,1)
    val starts = r.by(chunkSize).take(nchunks)
    val ends = starts.map(_-1).drop(1) :+r.end
    starts.zip(ends).map(x =>x._1 to x._2)
  }

  def main(args: Array[String]): Unit = {
    println("**********************************************")
  }

}

