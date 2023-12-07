package com.qianfeng.statics

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 基于Mongodb中的rating表中数据进行下面三个指标的分析
 * ● 统计所有历史数据当中每个商品的评分数次数，并存储到MongoDB的RateMoreProducts集合中。
 * ● 统计以月为单位拟每个商品的评分数次数，并存储到MongoDB的RateMoreRecentlyProducts集合中。
 * ● 统计每个商品的平均评分的分数，并存储到MongoDB的AverageProducts集合中。
 */

  //封装当前样例类进行数据存储
//1.提供mongodb的连接信息
case class MongoConfig(url:String,db:String)
//2.从Mongodb回复Rating数据
case class Rating(userId:Int,productId:Int,rating:Double,timestamp:Long)
object StaticsRecommender {

  /*
  提供静态推荐
   */
  def main(args: Array[String]): Unit = {
    //提供Mongodb链接配置i信息
    val configMap = Map(
      "url"->"mongodb://192.168.10.101:27017/recommender",
      "db"->"recommender"
    )
    //定义一些集合尝
    val RATING_COLLECTION = "Rating"
    //RateMoreProducts ---> 历史数据中每个商品的评分次数
    val RATE_MORE_COLLECTION = "RateMoreProducts"
    //RateMoreRecentlyProducts --> 以月为单位统计商品的评分次数
    val RATE_MORE_RECENTLY_COLLECTION = "RateMoreRecentlyProducts"
    //AverageProducts --> 统计每个商品的平均评分
    val AVERAGE_COLLECTION = "AverageProducts"
    //提供Mongodb链接隐式参数
    implicit val config = MongoConfig(configMap.getOrElse("url",""),configMap.getOrElse("db",""))
    //创建SparkSQL对象进行数据读取和写入
    val spark = SparkSession.builder().appName("StaticsRecommender")
      .master("local[*]").getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //读取Mongodb中Rating中的数据
    val ratingDF: DataFrame = spark.read.option("uri", config.url).option("collection",RATING_COLLECTION)
      .format(source = "com.mongodb.spark.sql").load().as[Rating].toDF()
    //将读取到的数据转换为一张表以进行分析操作
    ratingDF.createOrReplaceTempView("ratings")
    //分析指标1：统计所有历史数据当中每个商品的评分数次数，并存储到MongoDB的RateMoreProducts集合中。
    val rateMoreDF = spark.sql(
      """
        |select
        |productId,
        |count(*) count
        |from ratings
        |group by productId
        |""".stripMargin)
    rateMoreDF.show()
    //需要将分析之后的数据写入到MongoDB中
    writeDataToMongoDB(rateMoreDF,RATE_MORE_COLLECTION)

//    分析指标2：统计以月为单位拟每个商品的评分数次数，并存储到MongoDB的RateMoreRecentlyProducts集合中。

    //第一种就是提供UDF函数，使用UDF函数进行处理

    //1.需要提供一个日期格式化类
    val sdf = new SimpleDateFormat("YYYYMM")
    spark.udf.register("getMonth",(timestamp:Long)=>sdf.format(new Date(timestamp *1000)))
    val rateMoreRecentlDF = spark.sql(
      """
        |select
        |productId,
        |count(*) count,
        |getMonth(timestamp) yearmonth
        |from ratings
        |group by productId,getMonth(timestamp)
        |""".stripMargin)
    rateMoreRecentlDF.show()
    writeDataToMongoDB(rateMoreRecentlDF,RATE_MORE_RECENTLY_COLLECTION)

    //第二种使用SQL中自带时间函数进行日期处理
//    spark.sql(
//      """
//        |select
//        |productId,
//        |count(*) count,
//        |from unixtime(cast(timestamp as bigint),"yyyyMM") yearmonth
//        |from ratings
//        |group by productId,from unixtime(cast(timestamp as bigint),"yyyyMM")
//        |""".stripMargin).show()
    //分析指标3：统计每个商品的平均评分的分数，并存储到MongoDB的AverageProducts集合中。
    val avgDF = spark.sql(
      """
        |select
        |productId,
        |avg(rating) avg
        |from ratings
        |group by productId
        |""".stripMargin
    )
    avgDF.show()
    writeDataToMongoDB(avgDF,AVERAGE_COLLECTION)
    spark.stop()
  }
  def  writeDataToMongoDB(df:DataFrame,collection:String)(implicit config: MongoConfig):Unit={
  //获取Mongodb数据的Client对象并提供操作员的Collection

  val client = MongoClient(MongoClientURI(config.url))
  val coll = client(config.db)(collection)

    //删除集合操作 --> 如果写入数据会重新计算 没那么这个操作可以提供
    coll.dropCollection()
    //写入当前数据
    df.write.option("uri",config.url).option("collection",collection)
      .mode(SaveMode.Overwrite).format(source = "com.mongodb.spark.sql").save()
    //创建索引
    coll.createIndex(MongoDBObject("productId"-> 1))
  }
}
//package com.qingfeng.statics
//
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import com.mongodb.casbah.commons.MongoDBObject
//import com.mongodb.casbah.{MongoClient, MongoClientURI}
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
///*
//基于Mongodb中的rating表中数据进行下面三个指标的分析
//● 统计所有历史数据当中每个商品的评分数次数，并存储到MongoDB的RateMoreProducts集合中。
//● 统计以月为单位拟每个商品的评分数次数，并存储到MongoDB的RateMoreRecentlyProducts集合中。
//● 统计每个商品的平均评分的分数，并存储到MongoDB的AverageProducts集合中。
// */
////需要封装样例类进行数据存储操作
////封装Mongodb的信息
//case class MongoConfig(url:String,db:String)
////从Mongodb恢复Rating表数据
//case class Rating(userId:Int,productId:Int,rating:Double,timestamp:Long)
//object StaticsRecommender {
//  /**
//   * 提供静态推荐
//   */
//  def main(args: Array[String]): Unit = {
//    // 提供Mongodb连接配置信息
//    val configMap = Map(
//      "url"->"mongodb://192.168.10.101:27017/recommender",
//      "db"->"recommender"
//    )
//    //定义一些集合常量
//    val RATING_COLLECTION = "Rating"
//    //RateMoreProducts  -->  历史数据中每个商品的平分次数
//    val RATE_MORE_COLLECTION = "RateMoreProducts"
//    //RateMoreRecentlyProducts  --> 以月为单位统计商品的平分次数
//    val RATE_MORE_RECENTLY_COLLECTION = "RateMoreRecentlyProducts"
//    //AverageProducts --> 统计每个商品的平均评分
//    val AVERAGE_COLLECTION = "AverageProducts"
//    //提供Mongodb连接隐式参数
//    implicit  val config = MongoConfig(configMap.getOrElse("url",""),configMap.getOrElse("db",""))
//    //创建SparkSQL对象进行数据读取和写入
//    val spark= SparkSession.builder().appName("StaticsRecommender")
//      .master("local[*]").getOrCreate()
//    //导入隐式转换
//    import spark.implicits._
//    //读取Mongodb中Rating集合中数据
//    val ratingDF = spark.read.option("uri", config.url).option("collection", RATING_COLLECTION)
//      .format("com.mongodb.spark.sql").load().as[Rating].toDF()
//    //将读取到数据转换为一张表以进行分析操作
//    ratingDF.createOrReplaceTempView("ratings")
//    //分析指标1：统计所有历史数据当中每个商品的评分数次数，并存储到MongoDB的RateMoreProducts集合中
//    val rateMoreDF = spark.sql(
//      """
//        |select
//        |productId,
//        |count(*) count
//        |from ratings
//        |group by productId
//        |""".stripMargin)
//    rateMoreDF.show()
//    //需要将分析之后的数据写入到MongoDB中
//    writeDataToMongoDB(rateMoreDF,RATE_MORE_COLLECTION)
//    //分析指标2：统计以月为单位拟每个商品的评分数次数，并存储到MongoDB的RateMoreRecentlyProducts集合中。
//    //第一种做法就是提供UDF函数，使用UDF函数进行处理
//    //1.需要提供一个日期格式化类
//    /*  val sdf = new SimpleDateFormat("YYYYMM")
//      spark.udf.register("getMonth",(timestamp:Long)=>sdf.format(new Date(timestamp * 1000)))
//      spark.sql(
//        """
//          |select
//          |productId,
//          |count(*) count,
//          |getMonth(timestamp) yearmonth
//          |from ratings
//          |group by productId,getMonth(timestamp)
//          |""".stripMargin).show()*/
//
//    //第二种使用SQL中中自带时间函数进行日期处理
//    val rateMoreRecentlDF = spark.sql(
//      """
//        |select
//        |productId,
//        |count(*) count,
//        |from_unixtime(cast(timestamp as bigint),"yyyyMM") yearmonth
//        |from ratings
//        |group by productId,from_unixtime(cast(timestamp as bigint),"yyyyMM")
//        |""".stripMargin)
//    rateMoreRecentlDF.show()
//    writeDataToMongoDB(rateMoreRecentlDF,RATE_MORE_RECENTLY_COLLECTION)
//
//    //分析指标3:统计每个商品的平均评分的分数，并存储到MongoDB的AverageProducts集合中。
//    val avgDF = spark.sql(
//      """
//        |select
//        |productId,
//        |avg(rating) avg
//        |from ratings
//        |group by productId
//        |""".stripMargin)
//    avgDF.show()
//    //写入数据到Mongodb中
//    writeDataToMongoDB(avgDF,AVERAGE_COLLECTION)
//    spark.stop()
//  }
//
//  /**
//   * 通用写入Mongodb数据的方法
//   * @param df  分析之后的数据
//   * @param collection  要写入的集合
//   * @param config  Mongodb连接配置
//   */
//  def writeDataToMongoDB(df:DataFrame,collection:String)(implicit config: MongoConfig):Unit={
//    //获取Mongodb数据的Client对象并提供操作的Collection
//    val client = MongoClient(MongoClientURI(config.url))
//    val coll = client(config.db)(collection)
//    // 删除集合操作 --> 如果写入数据会重新计算那么这个操作可以提供
//    coll.dropCollection()
//    //写入数据
//    df.write.option("uri",config.url).option("collection",collection)
//      .mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save()
//    //创建索引
//    coll.createIndex(MongoDBObject("productId"->1))
//  }
//}

