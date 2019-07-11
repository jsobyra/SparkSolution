package event

case class RandomEvent(productName: String, productPrice: Double, productCategory: String, ipAddress: String, purchaseDate: String)

case class TopFrequentlyPurchasedCategory(category: String, frequency: Int)

case class TopFrequentlyPurchasedProductPerCategory(category: String, product: String, frequency: Int)

case class IpBlock(network: String, geoId: Int)

case class IpBlockCountry(network: String, countryName: String)






case class GeonameCountry(geonameId: Int, countryName: String)



case class IpBlockCountryRDD(network: String, countryName: String)

case class TopSpendingCountry(spending: Double, country: String)


