package idv.jack.example.spark.rdd
import org.apache.spark._

object WordCount {
  
  def main(args: Array[String]){
       val sparkConf = new SparkConf().setAppName("Word Count Test")
                                      //.setMaster("local")
                                      
       val sc = new SparkContext(sparkConf)
       val sourceRDD = sc.textFile("/wordcount.txt")
       
       val flatMapRDD = sourceRDD.flatMap(f => f.split(" "))
       val mapToPairRDD = flatMapRDD.map(word => (word, 1))
       val reduceRDD = mapToPairRDD.reduceByKey((x, y) => x + y)
       
       val resultList = reduceRDD.collect()
       
       for(result <- resultList){
          println(result._1 + ":" + result._2)
         }
       
       
  }
  
  
}