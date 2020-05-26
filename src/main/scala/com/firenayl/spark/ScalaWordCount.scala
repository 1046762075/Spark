package com.firenayl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: ScalaWordCount</p>
 * Description：
 * date：2020/5/25 20:39
 */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines = sc.textFile(args(0))
    //切分压平
    val words = lines.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne = words.map((_, 1))
    //按key进行聚合
    val reduced = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
