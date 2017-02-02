

import org.apache.spark.{SparkConf, SparkContext}





/**
  * Created by Mayanka on 09-Sep-15.
  */
object SparkWordCount {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir","C:\\Users\\Manikanta\\Documents\\UMKC Subjects\\PB\\hadoopforspark");

    val sparkConf = new SparkConf().setAppName("SparkActions").setMaster("local[*]").set("spark.driver.host", "localhost")

    val sc = new SparkContext(sparkConf)

    val input=sc.textFile("log")



    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    val date = output.collect().filter(f =>
      f._1.length() == 8 && f._1.charAt(2) == '/' && f._1.charAt(5) == '/'
    )

    var number = 0

    date.foreach(f => number += 1)

    println("Number of days logged" + number)

  }

}
