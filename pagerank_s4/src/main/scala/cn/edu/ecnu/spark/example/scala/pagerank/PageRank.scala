package cn.edu.ecnu.spark.example.scala.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File
import java.util.Date

object PageRank {
  def run(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("PageRank")
    conf.setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    val iterateNum = 20
    val factor = 0.85

    //    val text = sc.textFile("inputFilePath")
//    System.out.println(args(0))
//    System.out.println(args(1))
    val text = sc.textFile(args(0))

//    System.out.println("==================================")

    val links = text.map(line => {
      val tokens = line.split(" ")
      var list = List[String]()
      for (i <- 2 until tokens.size by 2) {
        list = list :+ tokens(i)
      }
      (tokens(0), list)
    }).cache()

    val N = 10
//    System.out.println(N)

    var ranks = text.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toDouble)
    })

    for (iter <- 1 to iterateNum) {
      val contributions = links
        .join(ranks)
        .flatMap {
          case (page, (links, rank)) =>
            links.map(dest => (dest, rank / links.size))
        }

      ranks = contributions
        .reduceByKey(_+_)
        .mapValues(v => (1 - factor) * 1.0 / N + factor * v)
    }

    ranks.foreach(t => println(t._1 + " ", t._2.formatted("%.5f")))
    ranks.saveAsTextFile(args(1))

    sc.stop()
  }
  def main(args: Array[String]): Unit = {
    var startTime =new Date().getTime
    run(args)
    var endTime =new Date().getTime
    println("程序运行时间：" + (endTime - startTime) + "ms") //单位毫秒
  }
}
