package invertedindex

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class InvertedIndexSuite extends FunSuite with BeforeAndAfterAll {
    private var conf : SparkConf = _
    private var sc : SparkContext = _

    lazy val testObject = new InvertedIndex()

    override def beforeAll() {
        conf = new SparkConf()
                        .setMaster("local")
                        .setAppName("test-invertedindex")
        sc = new SparkContext(conf)
    }

    override def afterAll() {
        sc.stop()
    }

    test("instantiate testObject") {
        val instantiated = try {
            testObject
            true
        } catch {
            case _: Throwable => false
        }

        assert(instantiated, "Can not instantiate a InvertedIndex object")
    }

    test("create inverted index for single column") {
        val text = "Today is Monday Tomorrow is fine"
        val column = sc.parallelize(text.split(' '))
        val result = testObject.createInvertedIndex(column)

        assert(result.collect.contains(("is", List(1, 4))))
    }

    test("calculate frequency") {
        val text = "Today is Monday Tomorrow is fine"
        val column = sc.parallelize(text.split(' '))
        val result = testObject.getInvertedIndexWithFrequency(column)

        assert(result.collect.contains(("is", List(1, 4), 2.0/6)))
    }

}
