import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

//read data
val clients = sqlContext.read.orc("clients.orc").persist(StorageLevel.MEMORY_ONLY)
val transactions = sqlContext.read.orc("transactions.orc").persist(StorageLevel.MEMORY_ONLY)

/* joins */

val transactionsWithClients = transactions.join(clients, clients("client_id") === transactions("client_id")).persist(StorageLevel.MEMORY_ONLY)

//note: nobody has multiple cards of the same brand
val clientsWithCards = clients.join(
	transactions.select($"client_id", $"pan", $"card_brand").distinct, 
	clients("client_id") === transactions("client_id")
).persist(StorageLevel.MEMORY_ONLY)

val clientTxInfo = transactions.groupBy($"client_id").agg(
	avg($"amount").alias("avg_tx"), sum($"amount").alias("sum_tx"), count($"amount").alias("count_tx")
).persist(StorageLevel.MEMORY_ONLY)

val clientsWithTxInfo = clientTxInfo.join(clients, clients("client_id") === transactions("client_id")).persist(StorageLevel.MEMORY_ONLY)

/* helper functions */

def printMaxMin(df: DataFrame, col: String) {
	df.agg(min($"$col"), max($"$col")).show(100, false)
}

def printClientStats(df: GroupedData) {
	df.agg(
		count($"gender"), avg($"income"), avg($"investment_rev"), 
		avg($"education"), avg($"activity_level")
	).show(100, false)
}

def getTxAmountStats(df: GroupedData): DataFrame = {
	df.agg(
		avg($"amount"), sum($"amount"), count($"amount"),
		(sum($"amount") / countDistinct(clients("client_id"))).alias("client_avg_sum"), 
		(count($"amount") / countDistinct(clients("client_id"))).alias("client_avg_count")
	)
}

def printTxAmountStats(df: GroupedData) {
	getTxAmountStats(df).show(100, false)
}

def printTxStatsByMerchCat(df: DataFrame) {
	printTxAmountStats(df.groupBy($"merch_cat_class"))
}

def printTxStatsByMonthWithGroup(df: DataFrame, col: String) {
	getTxAmountStats(df.groupBy($"$col", month($"time").alias("month"))).sort($"$col", $"month").show(100, false)
}

/* analysis */
println

println("Transaction time range:")
printMaxMin(transactions, "time")

println("Client stats:")
printClientStats(clients.groupBy())

println("Transaction amounts:")
printTxAmountStats(transactionsWithClients.groupBy())

println("Transaction amounts by month:")
printTxAmountStats(transactionsWithClients.groupBy(month($"time")))

println("Transaction amounts by day of month:")
printTxAmountStats(transactionsWithClients.groupBy(dayofmonth($"time")))

println("Client stats by gender:")
printClientStats(clients.groupBy($"gender"))

println("Transaction amounts by gender:")
printTxAmountStats(transactionsWithClients.groupBy($"gender"))

println("Client age range:")
printMaxMin(clients, "age")

println("Client stats by age:")
printClientStats(clients.groupBy(floor($"age" / 10)))

println("Transaction amounts by age:")
printTxAmountStats(transactionsWithClients.groupBy(floor($"age" / 10)))

println("Transaction amounts by age per month:")
printTxStatsByMonthWithGroup(transactionsWithClients.withColumn("age_flr", floor($"age" / 10)), "age_flr")

println("Client stats by education:")
printClientStats(clients.groupBy($"education"))

println("Transaction amounts by education:")
printTxAmountStats(transactionsWithClients.groupBy($"education"))

println("Transaction amounts by education per month:")
printTxStatsByMonthWithGroup(transactionsWithClients, "education")

println("Client stats by activity_level:")
printClientStats(clients.groupBy($"activity_level"))

println("Transaction amounts by activity_level:")
printTxAmountStats(transactionsWithClients.groupBy($"activity_level"))

println("Transaction amounts by activity_level per month:")
printTxStatsByMonthWithGroup(transactionsWithClients, "activity_level")

println("Client stats by card_brand:")
printClientStats(clientsWithCards.groupBy($"card_brand"))

println("Transaction amounts by card_brand:")
printTxAmountStats(transactionsWithClients.groupBy($"card_brand"))

println("Merchant list:")
transactions.groupBy($"merch_name").agg(first($"merch_cat_class").alias("cat_class"), first($"merch_cat_descr").alias("cat_descr"), count($"merch_name")).distinct.sort($"cat_class", $"cat_descr").show(100, false)

println("Transaction amounts by merchant category:")
printTxStatsByMerchCat(transactionsWithClients)

println("Gender A transaction amounts by merchant category:")
printTxStatsByMerchCat(transactionsWithClients.filter($"gender" === "A"))

println("Gender B transaction amounts by merchant category:")
printTxStatsByMerchCat(transactionsWithClients.filter($"gender" === "B"))

println("Transaction amounts by merchant category per month:")
printTxStatsByMonthWithGroup(transactionsWithClients, "merch_cat_class")

println("Fraud transaction list:")
transactionsWithClients.filter($"fraud_result" === true).show(100, false)
