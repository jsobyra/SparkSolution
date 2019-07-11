package event

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkAppRDD {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkAppRdd")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ipBlocks = sc.textFile("GeoLite2-Country-Blocks-IPv4.csv")
      .map(toIpBlock)
      .filter(x => x != null)
      //.load("hdfs://172.17.0.2:8020/user/cloudera/ips/ips.csv")

    val geonameCountries = sc.textFile("GeoLite2-Country-Locations-en.csv")
      .map(toGeonameCountry)
      .map(x => (x.geonameId, x))
    //.load("hdfs://172.17.0.2:8020/user/cloudera/locations/locations.csv")

    val networks = ipBlocks.map(ipBlock => ipBlock.network).collect().toList

    val randomEvents = sc.textFile("test2.csv")
      .map(toRandomEvent)
    //.load("hdfs://172.17.0.2:8020/user/cloudera/events.csv")

    val mappedRandomEvents = randomEvents.map(x => mapIpToNetwork(x, networks)).map(x => (x.ipAddress, x))

    val purchasedCategories = randomEvents
      .map(event => (event.productCategory, 1))
      .reduceByKey((acc, n) => acc + n)
      .sortBy(x => -x._2)
      .map(x => event.TopFrequentlyPurchasedCategory(x._1, x._2))


    val purchasedProductPerCategory = randomEvents
      .map(event => ((event.productCategory, event.productName), 1))
      .reduceByKey((acc, n) => acc + n)
      .reduceByKey((n1, n2) => if (n1 > n2) n1 else n2)
      .map(x => event.TopFrequentlyPurchasedProductPerCategory(x._1._1, x._1._2, x._2))
      .sortBy(x => -x.frequency)
      .take(10)

    val ipBlockCountries = ipBlocks
      .map(x => (x.geoId, x))
      .join(geonameCountries)
      .map(x => (x._2._1.network, event.IpBlockCountry(x._2._1.network, x._2._2.countryName)))

    val spendingCountries = mappedRandomEvents
      .join(ipBlockCountries)
      .map(x => (x._2._2.countryName, x._2._1.productPrice))
      .reduceByKey((acc, n) => acc + n)
      .map(x => event.TopSpendingCountry(x._2, x._1))
      .sortBy(x => -x.spending)
      .take(10)

    spendingCountries.foreach(println)

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/results"
    val table1 = "result1"
    val table2 = "result2"
    val table3 = "result3"

    purchasedCategories.register
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
      .write.option("driver", "com.mysql.jdbc.Driver").mode("append").jdbc(url, table3, prop)*/

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

  def toRandomEvent(row: String): RandomEvent = {
    RandomEvent(row.split(",")(0), row.split(",")(1).toDouble,
      row.split(",")(2), row.split(",")(3), row.split(",")(4))
  }

  def ipv4ToLong(ip: String): Long = {
    ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0, 8, 16, 24)).map(xi => xi._1 << xi._2).sum
  }

  def toGeonameCountry(row: String): GeonameCountry = {
    GeonameCountry(row.split(",")(0).toInt, row.split(",")(5))
  }

  def toIpBlock(row: String): IpBlock = {
    if (row.split(",")(0) != null && row.split(",")(1) != "") IpBlock(row.split(",")(0), row.split(",")(1).toInt)
    else null
  }
}
