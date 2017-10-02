/**
This is to get the inverted inde of field values and its corresponding frequency.
To build: 
    sbt package
Usage:
    spark-submit --master spark://elxa7d4n1g2:7077 
            　　--class inverted.InvertedIndex
　　　　　　　　　　　　　　target/scala-2.11/inverted-index_2.11-0.1.0.jar inputfile [outputfile]
 option configs:
      　　　　　　　　--driver-memory 1G
　　　　　　　　　　　　　　--executor-memory 1G
　　　　　　　　　　　　　　--conf spark.eventLog.enabled=True  
*/
package invertedindex

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import java.io._

/** The main class */
object InvertedIndex extends InvertedIndex {
    @transient lazy val conf: SparkConf = new SparkConf()
                                    .setAppName("InvertedIndex")
                                    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    @transient lazy val sc = new SparkContext(conf)

    case class ResultRow(fieldValue: String, indices: List[Long], freq: Double)

    def main(args: Array[String]) {
        var outputFileName: String = "invertedindex_parquet"

        try{
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._
            val conf = new Configuration()
            val fileSystem = FileSystem.get(conf)
 
            // Check and read input filename and output filename
            if (args.length < 1) throw new RuntimeException("Needs at least one argument:INPUT_FILE_URI!")
            if (args.length > 1) outputFileName = args(1)

            val linesDF = sqlContext.read.parquet(args(0))
            val outputFilePath = new Path(outputFileName)
            // Check whether file exists and delete old file
            if (fileSystem.exists(outputFilePath)) {
                fileSystem.delete(outputFilePath)
            }


            val lines = linesDF.rdd
            // skip the first column which is the unique key
            (1 until linesDF.first.size).foreach(columnId => {
                val column = lines.map(line => line(columnId).toString)
                val resultDF = getInvertedIndexWithFrequency(column)
                                  .map({ case (fieldValue, indices, freq) => ResultRow(fieldValue, indices, freq)})
                                  .toDF()
                resultDF.write.mode(SaveMode.Append).parquet(outputFileName)
            })
        }finally{
           sc.stop()
        }
    }
}



/** The inverted indexing and frequency calculation methods */
class InvertedIndex extends Serializable {
    def createInvertedIndex(column: RDD[String]): RDD[(String, List[Long])] = {
        val fieldWithIndex = column.zipWithIndex.partitionBy(new HashPartitioner(20))
        //val fieldWithIndex = column.zipWithIndex
        fieldWithIndex.groupBy(_._1).map({
             case (fieldValue, groupedIndexValues) => {
                  val indices = groupedIndexValues.map({case (fieldValue, index) => index}).toList
                  (fieldValue, indices)
             }
        })
    }

    def getInvertedIndexWithFrequency(column: RDD[String]): RDD[(String, List[Long], Double)] = {
        val invertedIndex = createInvertedIndex(column)
        val rowCount = invertedIndex.map({case (fieldValue, indices) => (fieldValue, indices, indices.size)})
        val rowSum = rowCount.map(_._3).reduce(_+_)
        val indexWithFrequency = rowCount.map({case (fieldValue, indices, count) => (fieldValue, indices, count*1.0/rowSum)})
        indexWithFrequency
    }
}


