package event

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/*
--deploy-mode cluster  --driver-memory 2048mb --executor-memory 2048mb --executor-cores 1
docker cp target/scala-2.10/ScalaEvent-assembly-0.1.jar 6b84cc3607f8:/spark-job.jar
*/
object SparkAppDataset {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val ipBlocks = sqlContext.read.format("com.databricks.spark.csv")
      .load("hdfs://172.17.0.2:8020/user/cloudera/ips/ips.csv")
      .map(toIpBlock).filter(x => x != null).toDS()

    val geonameCountries = sqlContext.read.format("com.databricks.spark.csv")
      .load("hdfs://172.17.0.2:8020/user/cloudera/locations/locations.csv")
      .map(toGeonameCountry).toDS()

    val networks = ipBlocks.map(ipBlock => ipBlock.network).collect().toList

    val randomEvents = sqlContext.read.format("com.databricks.spark.csv")
      .load("hdfs://172.17.0.2:8020/user/cloudera/events.csv")
      .map(toRandomEvent).toDS()

    val mappedRandomEvents = randomEvents.map(x => mapIpToNetwork(x, networks)).toDS()


    val purchasedCategories = randomEvents
      .map(event => (event.productCategory, 1))
      .groupBy(_._1)
      .reduce((a, b) => (a._1, a._2 + b._2))
      .map(x => x._2)
      .map(x => event.TopFrequentlyPurchasedCategory(x._1, x._2))
      .toDF().sort($"frequency".desc)
      .limit(10)
      .map(x => event.TopFrequentlyPurchasedCategory(x.getString(0), x.getInt(1)))


    val purchasedProductPerCategory = randomEvents
      .map(event => ((event.productCategory, event.productName), 1))
      .groupBy(_._1)
      .reduce((a, b) => (a._1, a._2 + b._2))
      .map(x => x._2)
      .map(x => (x._1._1, x._1._2, x._2))
      .groupBy(_._1)
      .reduce((a, b) => {
        if (a._3 > b._3) (a._1, a._2, a._3)
        else (a._1, a._2, b._3)
      })
      .map(x => event.TopFrequentlyPurchasedProductPerCategory(x._2._1, x._2._2, x._2._3))
      .toDF().sort($"frequency".desc)
      .limit(10)
      .map(x => event.TopFrequentlyPurchasedProductPerCategory(x.getString(0), x.getString(1), x.getInt(2)))

    val ipBlockCountries = ipBlocks
      .joinWith(geonameCountries, $"geoId" === $"geonameId")
      .map(x => event.IpBlockCountry(x._1.network, x._2.countryName))
      .toDS()

    val spendingCountries = mappedRandomEvents
      .joinWith(ipBlockCountries, $"ipAddress" === $"network")
      .map(x => (x._1.productPrice, x._2.countryName))
      .groupBy(_._2)
      .reduce((a, b) => (a._1 + b._1, a._2))
      .map(x => x._2)
      .map(x => event.TopSpendingCountry(x._1, x._2))
      .toDF().sort($"spending".desc)
      .limit(10)
      .map(x => event.TopSpendingCountry(x.getDouble(0), x.getString(1)))

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/results"
    val table1 = "result1"
    val table2 = "result2"
    val table3 = "result3"

    purchasedCategories.toDF()
      .withColumnRenamed("category", "product_category")
      .withColumnRenamed("frequency", "frequence")
      .write.option("driver", "com.mysql.jdbc.Driver").mode("append").jdbc(url, table1, prop)

    purchasedProductPerCategory.toDF()
      .withColumnRenamed("category", "product_category")
      .withColumnRenamed("product", "product_name")
      .withColumnRenamed("frequency", "frequence")
      .write.option("driver", "com.mysql.jdbc.Driver").mode("append").jdbc(url, table2, prop)

    spendingCountries.toDF()
      .withColumnRenamed("spending", "sum")
      .withColumnRenamed("country", "country_iso_code")
      .write.option("driver", "com.mysql.jdbc.Driver").mode("append").jdbc(url, table3, prop)

  }

  def mapIpToNetwork(event: RandomEvent, networks: List[String]): RandomEvent = {
    val network = networks.find(x => ipInRange(event.ipAddress, x))
    if (network.nonEmpty) RandomEvent(event.productName, event.productPrice, event.productCategory, network.get, event.purchaseDate)
    else event
  }

  def ipInRange(ip: String, network: String): Boolean = {
    try {
      getHighAddress(network) >= ipv4ToLong(ip) && ipv4ToLong(ip) >= getLowAddress(network)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  private def getLowAddress(range: String) = ipv4ToLong(new SubnetUtils(range).getInfo.getLowAddress)

  private def getHighAddress(range: String) = ipv4ToLong(new SubnetUtils(range).getInfo.getHighAddress)

  def toRandomEvent(row: Row): RandomEvent = {
    RandomEvent(row.getAs[String](0), row.getAs[String](1).toDouble, row.getAs[String](2), row.getAs[String](3), row.getAs[String](4))
  }

  def ipv4ToLong(ip: String): Long = {
    ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0, 8, 16, 24)).map(xi => xi._1 << xi._2).sum
  }

  def toGeonameCountry(row: Row): GeonameCountry = {
    GeonameCountry(row.getAs[String](0).toInt, row.getAs[String](5))
  }

  def toIpBlock(row: Row): IpBlock = {
    if (row.get(0) != null && row.getString(1) != "") IpBlock(row.getString(0), row.getString(1).toInt)
    else null
  }
}
