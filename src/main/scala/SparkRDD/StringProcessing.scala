package SparkRDD

object StringProcessing {
  def main(args: Array[String]): Unit = {
    //One record from order
    //Fields:order_id,order_date,order_customer_id,order_status
    val o = "1,2013-07-25 00:00:00.0,11599,CLOSED"

    //Get Date using substing
    val firstComma = o.indexOf(",")
    println("Index of first , is "+ firstComma)
    val secondComma = o.indexOf(",",firstComma+1)
    println("Index of second , is "+ secondComma)
    println("Date using substring "+ o.substring(firstComma+1,secondComma))

    //Get date using split
    val order_date: String = o.split(",")(1) //select variable name press alt+enter you will get add type annotation
    println("Order date using split "+ order_date)

    //Get order_id and order_customer_id as integers
    val order_id = o.split(",")(0).toInt
    println("Order id is "+ order_id)
    val order_customer_id = o.split(",")(2).toInt
    println("order customer id is "+ order_customer_id)

    //compare whether order_status is CLOSED or not
    val order_status = o.split(",")(3)
    val isClosed = order_status == "CLOSED"
    println("Is status of order id "+ order_id + " closed? "+ isClosed )

    //get date as integer in the format of YYYYMMDD
    val orderDateInt = order_date.substring(0,10).replace("-","").toInt
    println("order date in YYYYMMDD format " + orderDateInt)

  }

}
