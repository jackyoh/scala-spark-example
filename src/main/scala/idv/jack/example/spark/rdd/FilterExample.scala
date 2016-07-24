package idv.jack.example.spark.rdd
import org.apache.spark._

object FilterExample {
  def main(args:Array[String]){
     val sparkConf = new SparkConf().setAppName("FilterTest")
                                    .setMaster("local")
    
     val sparkContext = new SparkContext(sparkConf)
     val sourceFileRDD = sparkContext.textFile("/home/user1/wordcount.txt")
    
     val filterRDD = sourceFileRDD.filter(word => word.contains("aaaa"))
     
     val resultList = filterRDD.collect();
     
     for(result<-resultList){
         println(result)
       }
  }
}