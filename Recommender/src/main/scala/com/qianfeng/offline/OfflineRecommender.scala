package com.qianfeng.offline

import breeze.linalg.rank
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/*
离线推荐服务和相似商品加工
 */
//Mongodb链接信息封装
case class MongoConfig(url:String,db:String)
//样例类
case class ProductRating(userId:Int,productId:Int,rating:Double,timestamp:Long)
//提供商品封装信息
case class Recommendation(productId:Int,score:Double)
//封装商品相似矩阵信息
case class ProductRecs(productId:Int,recs:Seq[Recommendation])
//封装用户的信息
case class UserRecs(userId:Int,recs:Seq[Recommendation])
object OfflineRecommender {

  def main(args: Array[String]): Unit = {


    val configMap = Map(
      "url"->"mongodb://192.168.10.101:27017/recommender",
      "db"->"recommender"
    )
    //隐式转换函数
    implicit val config = MongoConfig(configMap.getOrElse("url",""),configMap.getOrElse("db",""))
    //提供代码中所需要的常量
    // --》 用户推荐存储再Mongodb中的集合
    val USER_RECS_COLLECTION = "UserRecs"
    //--> 商品相似矩阵存储再Mongodb中集合
    val PRODUCT_RECS_COLLECTION = "ProductRecs"
    // --> 用户评分的集合
    val RATING_COLLECTION = "Rating"
    //--> 对用户的最大推荐数量
    val MAX_RECOMMEND_NUMBER = 20
    //提供SparkSQL对象的创建
    val spark = SparkSession.builder().appName("OfflineRecommender")
      .master("local[*]").getOrCreate()
    //需要导入隐式转换
    import spark.implicits._
    //读取Mongodb中Rating集合的数据，然后转换为RDD进行处理
    val readRating: RDD[ProductRating] = spark.read.option("uri",config.url).option("collection",RATING_COLLECTION)
      .format("com.mongodb.spark.sql").load().as[ProductRating].rdd
    //将当前RDD中的数据转换为三元组进行计算操作
    val ratingRDD: RDD[(Int, Int, Double)] = readRating.map(line => (line.userId, line.productId, line.rating))
    //提供算法的训练集的构建 --> 需要将ratingRDD中的数据转存到MLib包中进行存储
    val trainData: RDD[Rating] = ratingRDD.map(x => org.apache.spark.mllib.recommendation.Rating(x._1, x._2, x._3))
    //需要对当前的userId 和ProductId进行去重操作
    val userRDD: RDD[Int] = ratingRDD.map(x => x._1).distinct()//去重后的用户ID信息
    val productRDD: RDD[Int] = ratingRDD.map(x => x._2).distinct() //去重后的商品ID信息
    //需要将用户ID和商品ID进行整合，形成一个用户-商品ID矩阵 --> cartesian笛卡尔积计算
    val userProduct: RDD[(Int, Int)] = userRDD.cartesian(productRDD)
    //构建ASL算法模型 --> 隐式因子的个数、迭代次数、正则化次数
    val(rank,iterations,regex) = (10,10,0.01)
    //引入mllib包下ASL算法进行模型训练
    val aslmode: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, regex)
    //预测 --》 使用userProduct（笛卡尔积）进行预测
    val preRDD: RDD[Rating] = aslmode.predict(userProduct)
    //对预测的结果进行过滤和封装
    val userRecsDF: DataFrame = preRDD.filter(r => r.rating > 0) //将预测的评分大于0的商品过滤出来
      .map(rat => {
        //将mllib中的提供系统类Rating封装到与之怒中方便后续操作
        (rat.user, (rat.product, rat.rating)) //等价于将数据封装成（userId，（productId，rating））

      }).groupByKey()
      .map(x => {
        //将这个计算之后的结果封装到UserRecs中
        UserRecs(x._1, x._2.toList //转换为List集合的目的，就是为了方便操作)
          .sortWith(_._2 > _._2) //对存储的评分结果进行一个降序排序
          .take(MAX_RECOMMEND_NUMBER) //获取评分最高的前20个数据
          .map(x => Recommendation(x._1, x._2))) // 将商品ID和预测得到的得分封装到Recommendation中
      }).toDF() //根据userId进行分组 --》 得到这个用户下有哪些商品和商品评分
    userRecsDF.show(false)
    //将这个结果写入到Mongodb中
    writeDataToMongoDB(userRecsDF,USER_RECS_COLLECTION)

    //商品的相似度矩阵
    val productFeature: RDD[(Int, DoubleMatrix)] = aslmode.productFeatures.map(x => {
      (x._1, new DoubleMatrix(x._2)) //获取出商品id 和对应商品的特征向量
    })
    //需要获取每一个商品的余弦相似度
    val ProductRecsDF: DataFrame = productFeature.cartesian(productFeature) //得到的就是商品信息向量的笛卡尔积
      .filter(x => x._1._1 != x._2._1) //过滤掉两个相同商品
      .map(x => {
        //需要获取当前商品的特征向量
        val consin = consinsim(x._1._2, x._2._2)
        //封装数据(p1商品ID,(P2商品id p1和p2的余弦相似度))
        (x._1._1, (x._2._1, consin))
      }).filter(x => x._2._2 > 0.6) //过滤掉余弦度小于0.6的商品
      .groupByKey()
      .map(x => {
        //封装到样例类中ProductRecs
        ProductRecs(x._1, //商品id)
          x._2.toList.map(x => Recommendation(x._1, x._2))
        )
      }).toDF() //根据商品的id进行分组

    //写入Mongodb中
    writeDataToMongoDB(ProductRecsDF,PRODUCT_RECS_COLLECTION)
  }
  //将数据写入Mongodb
  def writeDataToMongoDB(df:DataFrame,collection:String)(implicit config: MongoConfig):Unit={
    //获取Mongodb的Client对象和获取操作的结合
    val client = MongoClient(MongoClientURI(config.url))
    val coll = client(config.db)(collection)
    //多次测试 要进行频繁的读写 提供一个删除集合的操作
    coll.dropCollection()
    //写入数据到Mongodb中
    df.write.option("uri",config.url).option("collection",collection)
      .mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save()
  }
  def consinsim(product1:DoubleMatrix,product2:DoubleMatrix):Double={
  //相似公式 = y = (DXI*DYi)
    product1.dot(product2) / (product1.norm2()*product2.norm2())
  }
}
