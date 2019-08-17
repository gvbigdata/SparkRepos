import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sparkFileExample {

  def main(args: Array[String]): Unit = {
    //case class orders(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("sparkFileExample").getOrCreate()

    var inputfile = spark.conf.get("spark.input.file")

    println(inputfile)

    var outputfile = spark.conf.get("spark.output.file")
    println(outputfile)
    val rdd = spark.sparkContext.textFile(inputfile)
    val splitrdd = rdd.flatMap(_.split(" "))
    splitrdd.collect.foreach(println)
    val transformrdd = splitrdd
      .filter(_.startsWith("#"))
      .flatMap(x=>x.split(""))
      .filter(x=>x.contains("#")).map(x=> (x,1)).reduceByKey(_+_)

    transformrdd.saveAsTextFile(outputfile)

  }

}