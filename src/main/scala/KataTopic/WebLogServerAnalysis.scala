package KataTopic

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions.{col, count, desc}
import org.apache.spark.sql.functions._


case class LogRecord( clientIp: String, clientIdentity: String, user: String, dateTime: String, request:String,statusCode:Int, bytesSent:Long)

object WebLogServerAnalysis {

  def mySqlJdbcConn():Properties ={
    //Connection Properties
    val props = new Properties()
    props.setProperty("driver","com.mysql.cj.jdbc.Driver")  //  com.mysql.cj.jdbc.Driver
    props.setProperty("user","root")
    props.setProperty("password","root")
    //JDBC URL
    //val url ="jdbc:mysql://localhost:3306/learn_db?useSSL=false"
    //TableNM
    //var tableNM = "employee"
    //val sqlDF = spark.read.jdbc(url,tableNM,props)
    //sqlDF.show(10)
    return props
  }

  def convertToDate(dateStr: String): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")  //dd/MMM/yyyy
    val actualDate = simpleDateFormat.parse(dateStr)
      new SimpleDateFormat("yyyy-MM-dd").format(actualDate)
  }

  def getSparkSession(master:String,appName:String): SparkSession = {
    val conf:SparkConf  = new SparkConf()
        if(System.getProperties().getProperty("os.name").contains("Windows")){
          System.setProperty("hadoop.home.dir","C:\\Anil\\Learning\\hadoop")
          conf.set("spark.driver.host","localhost")
        }else
          {

          }

    if(System.getProperties().getProperty("os.name").contains("Windows")) {
      val spark: SparkSession = SparkSession.builder().config(conf).master(master).appName(appName).getOrCreate()
      return spark
    }
    else {
      val spark: SparkSession = SparkSession.builder().config(conf).master(master).appName(appName).enableHiveSupport().getOrCreate()
      return spark
    }
  }

  //val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)""".r
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)""".r

  //val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(\S*)" (\d{3}) (\S+)""".r


  def parseLogLine(record: String): LogRecord = {
        try {
              val res = PATTERN.findFirstMatchIn(record)
              if (res.isEmpty) {
                println("Rejected Log Line: " + record)
                LogRecord("Empty", "-", "-", "", "",  -1, -1)
              }
              else {
                //println("Valid")
                val m = res.get
                // NOTE:   HEAD does not have a content size.
                if (m.group(9).equals("-")) {
                  LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
                    m.group(5).concat(m.group(6)).concat(m.group(7)), m.group(8).toInt, 0)
                }
                else {
                  LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
                    m.group(5).concat(m.group(6)).concat(m.group(7)), m.group(8).toInt, m.group(9).toLong)
                }
              }
            }
        catch  {
            case e: Exception =>
              println("Exception on line:" + record + ":" + e.getMessage);
              LogRecord("Empty", "-", "-", "", "-", -1, -1)
        }
  }

  //// Main Spark Program
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)
    val log = Logger.getLogger("com.hortonworks.spark.Logs")

    if(args.length < 2){
      log.info("Main :: Invalid No.of arguments please pass arguments : Master & Filepath")
    }

    log.info("Started Logs Analysis")

    val spark:SparkSession = getSparkSession(args(0),"WebLogServerAnalysis")


    val logFile = spark.sparkContext.textFile(args(1))
    val accessLogs:RDD[LogRecord] = logFile.map(parseLogLine).filter(!_.clientIp.equals("Empty"))
    accessLogs.cache()

    log.info("# of Partitions %s".format(accessLogs.partitions.size))
    accessLogs.collect().foreach(println)

    log.info("# of Request per Status Code")
    accessLogs.map(l => (l.statusCode,1)).reduceByKey(_+_).sortByKey(false).take(10).foreach(println) //.collect().foreach(println)
    log.info("# of Request per date")
    //accessLogs.map(l => (convertToDate(l.dateTime.split(":")(0)),1)).reduceByKey(_+_).collect().foreach(println)
    accessLogs.map(l => (convertToDate(l.dateTime),1)).reduceByKey(_+_).sortBy(_._2,false).take(10).foreach(println)  //.collect().foreach(println)

    log.info("# of Number of Suspicious Records vs correct record  ")
    val totalLogRecords = accessLogs.count()
    println("Total Log Records count : "+totalLogRecords)
    val suspiciousLogRecords = accessLogs.filter(_.statusCode == 304)
    val correctLogRecords = accessLogs.filter(_.statusCode != 304)
    val percntSusrecords= (suspiciousLogRecords.count/totalLogRecords.toFloat)*100
    log.info("# Percentage correct record vs Total number of records ")
      println("% of Suspicious Log Records : "+percntSusrecords)
    val peccntCorrect = 100 - percntSusrecords
    println("% of correct Log Records : "+peccntCorrect)


    //DataFrame using
          //import spark.sqlContext.implicits._
          //accessLogs.toDF()
    log.info("******Using DataFrames******")
    val accessLogsConvrt = accessLogs.map(r => Row(r.clientIp,convertToDate(r.dateTime),r.request,r.statusCode,r.bytesSent))

    val wedLogSchema = new StructType().add("ClientIP",StringType)
                                  .add("ReqDate",StringType)
                                  .add("Request",StringType)
                                  .add("StatusCode",IntegerType)
                                  .add("BytesSent",LongType)

    val logDF= spark.createDataFrame(accessLogsConvrt,wedLogSchema)
    logDF.printSchema()
    logDF.show(10)

    val suspiciouslogDF= logDF.filter(logDF("StatusCode") === 304)
    val correctlogDF= logDF.filter(col("StatusCode") =!= 304)

    log.info("******Writing Dataframe to MySQL DB******")
    val url = "jdbc:mysql://localhost:3306/kata?useSSL=false"
    suspiciouslogDF.write.mode(SaveMode.Overwrite).jdbc(url,"SuspiciousLogDF",mySqlJdbcConn)
    correctlogDF.write.mode(SaveMode.Overwrite).jdbc(url,"AccessLogDF",mySqlJdbcConn)
    log.info("******End : Loaded to MySQL DB ******")

    log.info("******Started : Loading to Hive ******")
    spark.sql("Drop table if exists Hive_SuspiciousLog")
    correctlogDF.write.mode(SaveMode.Overwrite).saveAsTable("Hive_SuspiciousLog")
    correctlogDF.write.mode(SaveMode.Overwrite).partitionBy("ReqDate").saveAsTable("Hive_AccessLog")
    log.info("******End : Loaded to Hive******")

//    logDF.createOrReplaceTempView("AccessLogs")
//    spark.sql("SHOW TABLES").show(10)
//    //spark.sql("SELECT * FROM AccessLogs").show(10)
    spark.stop()

  }
}
