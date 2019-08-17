package SparkRDD
//> mapPartitionsWithIndex is similar to mapPartitions() but it provides second parameter index which keeps the track of partition.
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object mapPartitionWithIndexTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapPartitionTransformation").getOrCreate()
    val sc = spark.sparkContext
//

  }

}
