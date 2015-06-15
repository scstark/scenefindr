import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object sceneFindr {
  def main(args: Array[String]) {

    // setup the Spark Context named sc
    val conf = new SparkConf().setAppName("scenefindr")
    val sc = new SparkContext(conf)

    // folder on HDFS to pull the data from
    val folder_name = ":9000/user/sceneFindr/history"

    // function to convert a timestamp to a 30 minute time slot
    def convert_to_30min(timestamp: String): String = {
        val min30 = timestamp.slice(11,13).toInt/30*30
        timestamp.take(11) + f"${min30}%02d" + "00"
    }

    // read in the data from HDFS
    val file = sc.textFile(folder_name)

    // map each record into a tuple consisting of (time, price, volume)
    val ticks = file.map(line => {
                         val record = line.split(";")
                        (record(0), record(1).toDouble, record(2).toInt)
                                 })

    // apply the time conversion to the time portion of each tuple and persist it memory for later use
    val ticks_min30 = ticks.map(record => (convert_to_30min(record._1),
                                           record._2,
                                           record._3)).persist

    // compute the average price for each 30 minute period
    val price_min30 = ticks_min30.map(record => (record._1, (record._2, 1)))
                                 .reduceByKey( (x, y) => (x._1 + y._1,
                                                          x._2 + y._2) )
                                 .map(record => (record._1,
                                                 record._2._1/record._2._2) )

    // compute the total volume for each 30 minute period
    val vol_min30 = ticks_min30.map(record => (record._1, record._3))
                               .reduceByKey(_+_)

    // join the two RDDs into a new RDD containing tuples of (30 minute time periods, average price, total volume)
    val price_vol_min30 = price_min30.join(vol_min30)
                                     .sortByKey()
                                     .map(record => (record._1,
                                                     record._2._1,
                                                     record._2._2))

    // save the data back into HDFS
    price_vol_min30.saveAsTextFile(":9000/user/price_data_output_scala")
  }
}
