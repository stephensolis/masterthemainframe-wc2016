//db settings
/*val dbURL = "jdbc:db2://192.86.32.178:5035/DALLASB"
val dbProps = new java.util.Properties {
	setProperty("driver", "com.ibm.db2.jcc.DB2Driver")
	setProperty("user", "")
	setProperty("password", "")
}*/
val dbURL = "jdbc:postgresql://localhost/CARDUSR"
val dbProps = new java.util.Properties {
	setProperty("driver", "org.postgresql.Driver")
	setProperty("user", "postgres")
	setProperty("password", "postgres")
}

//converters
val toGender = udf((g: Int) => g match {
	case 0 => "A"
	case 1 => "B"
})

//dataframe for clients
val rawClients = sqlContext.read.jdbc(dbURL, "cardusr.client_info", dbProps)
val clients = rawClients.select(
	$"CONT_ID".cast("int").as("client_id"),
	toGender($"GENDER").as("gender"),
	$"AGE_YEARS".cast("decimal(4,2)").as("age"),
	$"HIGHEST_EDU".as("education"),
	$"ANNUAL_INVEST".cast("decimal(10,0)").as("investment_rev"),
	$"ANNUAL_INCOME".cast("decimal(10,0)").as("income"),
	$"ACTIVITY_LEVEL".as("activity_level"),
	$"CHURN".cast("boolean").as("discontinued")
)

//dataframe for transactions
val rawTransactions = sqlContext.read.jdbc(dbURL, "cardusr.sppaytb", dbProps)
val transactions = rawTransactions.select(
	$"ACAUREQ_HDR_CREDTT".cast("timestamp").as("time"),
	$"ACAUREQ_AUREQ_ENV_A_ID_ID".as("issuer_name"),
	$"ACAUREQ_AUREQ_ENV_M_ID_ID".as("strange_field"),
	$"ACAUREQ_AUREQ_ENV_M_CMONNM".as("merch_name"),
	$"ACAUREQ_AUREQ_ENV_CPL_PAN".cast("int").as("pan"),
	$"ACAUREQ_AUREQ_ENV_C_CARDBRND".as("card_brand"),
	$"ACAUREQ_AUREQ_TX_MRCHNTCTGYCD".cast("int").as("merch_cat_id"),
	$"ACAUREQ_AUREQ_TX_DT_TTLAMT".cast("decimal(6,2)").as("amount"),
	$"CONT_ID".cast("int").as("client_id"),
	$"MDM_POSTAL_CODE_ID".as("postal_code_id"),
	$"AGE".as("interest_grace_time"),
	$"AUTHORRESULT_RSPNT".as("auth_result"),
	$"FRAUD_VER_RESULT".cast("boolean").as("fraud_result")
)

//fetch MCC code list
val mcc_codes = (sqlContext.read.format("com.databricks.spark.csv")
	option("header", "true")
	option("inferSchema", "true")
	load("mcc_codes.csv"))

//join to transactions
val transactions_with_mcc = transactions.join(
	mcc_codes
		.withColumnRenamed("description", "merch_cat_descr")
		.withColumnRenamed("category", "merch_cat_class")
	, $"mcc" === $"merch_cat_id", "left_outer"
).drop("mcc")

//JSON export
clients.repartition(1).write.json("clients.json")
transactions_with_mcc.repartition(1).write.json("transactions.json")

//ORC export
clients.repartition(1).write.orc("clients.orc")
transactions_with_mcc.repartition(1).write.orc("transactions.orc")

//DB export
//clients.write.jdbc(dbURL, "cardusr.clients", dbProps)
//transactions.write.jdbc(dbURL, "cardusr.transactions", dbProps)
