package com.qianfeng.dataload


import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/*
加载当前数据到Mongodb中 --》 商品的数据和用户商品评分数据 进行封装加载到Mongodb下Recommender库下面
 */

//提供样例类进行数据的封装
//1.提供mongodb的连接信息
case class MongoConfig(url:String,db:String)
//2.提供数据文件封装的样例类 --》 商品数据Product
case class Product(productId:Int,name:String,imageUrl:String,categories:String,tags:String)
//3.提供数据文件封装的样例类 --》 用户商品评分数据Rating
case class Rating(userId:Int,productId:Int,rating:Double,timestamp:Long)
object DataLoader {

  //提供可以使用到的常量值
  val RATING_COLLECTION = "Rating"
  val PRODUCT_COLLECTION = "Product"

  def main(args: Array[String]): Unit = {
    //提供当前Mongodb的链接配置信息
    val configMap = Map(
      "url"->"mongodb://192.168.10.101:27017/recommender",
      "db"->"recommender")
    //提供了一个隐式转换参数 --》 自动参数 如果遇到需要使用是，会自动扫描这个参数
    implicit  val config = MongoConfig(configMap.getOrElse("url",""),configMap.getOrElse("db",""))
    //提供Spark SQL对象的创建进行对象的读取
    val spark = SparkSession.builder().appName("DataLoader").master("local[*]").getOrCreate()
    //提供当前Spark SQL所需要的隐式转换
    import spark.implicits._
    //读取当前Product的数据并转换为对应的表
    val productDF: DataFrame = spark.sparkContext.textFile(path = "src/main/resources/products.csv")
      .filter(_.length > 0)
      .map(line => {
        //数据的分隔符号进行数据的拆更操作
        val fileds: Array[String] = line.split("\\^")
        //读取数组中的数据并进行转换操作 封装到样例类中 --> 切分的数据在存储到数组中 2、3下标的数据是无效的 不需要的
        Product(fileds(0).trim.toInt, fileds(1).trim, fileds(4).trim, fileds(5).trim, fileds(6).trim)

      }).toDF()
    //读取当前Ratings的数据文件中的数据
    val ratingDF: DataFrame = spark.sparkContext.textFile(path = "src/main/resources/ratings.csv")
      .filter(_.length > 0)
      .map(line => {
        //数据的分隔符号进行数据的拆更操作
        val fileds: Array[String] = line.split("\\,")
        //读取数组中的数据并进行转换操作 封装到样例类中 --> 切分的数据在存储到数组中 2、3下标的数据是无效的 不需要的
        Rating(fileds(0).trim.toInt, fileds(1).trim.toInt, fileds(2).trim.toDouble, fileds(3).trim.toLong)
      }).toDF()
    //打印数据验证是否读取成功
    productDF.show(false)
    ratingDF.show(false)
    //提供一个方法 将两个读取到的数据productDF和ratingDF写入到Mongodb中去
    //如果定义了隐式参数，就不需要再传递隐式参数
    writeDataToMongoDB(productDF,ratingDF)
  }
  /*
  将product DF和ratingDF写入到Mongodb中
  @param
   */
  def writeDataToMongoDB(productDF:DataFrame,ratingDF:DataFrame)(implicit config: MongoConfig):Unit={
    //获取Mongodb的Client
    val client = MongoClient(MongoClientURI(config.url))
    //获取操作的集合
    val productCollection = client(config.db)(PRODUCT_COLLECTION)
    val ratingCollection = client(config.db)(RATING_COLLECTION)
    //分别将productDF和ratingDF写入到Mongodb中
    productDF.write.option("uri",config.url).option("collection",PRODUCT_COLLECTION)
      .mode(SaveMode.Overwrite).format(source = "com.mongodb.spark.sql").save()
   ratingDF.write.option("uri",config.url).option("collection",RATING_COLLECTION)
      .mode(SaveMode.Overwrite).format(source = "com.mongodb.spark.sql").save()

    //分别进行索引的创建
    productCollection.createIndex(MongoDBObject("productId"->1))
    ratingCollection.createIndex(MongoDBObject("productId"->1))
    ratingCollection.createIndex(MongoDBObject("userId"->1))
  }}
