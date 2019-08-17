package SparkRDD
//mapPartitions() can be used as an alternative to map() and foreach() .
//      > mapPartitions() can be called for each partitions while map() and foreach() is called for each elements in an RDD
//    > Hence one can do the initialization on per-partition basis rather than each element basis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object mapPartitionTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapPartitionTransformation").getOrCreate()
    val sc = spark.sparkContext
    val nyse = sc.wholeTextFiles("C:\\Anil\\Learning\\Spark\\data-master\\nyse\\nyse_data\\")
//    val nyseMapPartitions = nyse.mapPartitions(e =>{
//      e.map(nyseRec => {
//        val a = nyseRec.split(",")
//        ((a(1),a(0)),a(6).toLong)
//      })
//    })
     val data = sc.parallelize(List(1,2,3,4,5,6,7,8), 2)

    //Map:

    def sumfuncmap(numbers : Int) : Int =
    {
      var sum = 1

      return sum + numbers
    }

    data.map(sumfuncmap).collect.foreach(println)

    def sumfuncpartition(numbers : Iterator[Int]) : Iterator[Int] =
    {
      var sum = 1
      while(numbers.hasNext)
      {
        sum = sum + numbers.next()
      }
      return Iterator(sum)
    }

    data.mapPartitions(sumfuncpartition).collect.foreach(println)


  }

}
