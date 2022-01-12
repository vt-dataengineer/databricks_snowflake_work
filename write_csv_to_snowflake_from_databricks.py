
#read the csv file present on temporary location on databricks
df = spark.read.option("header", "true").csv("/tmp/TaxiZones.csv")
df.show(10)

#Using the Databricks Secrets API to store and encrypt the snowflake credentials
non_prod_user = dbutils.secrets.get(scope="kv_name",key="user")
non_prod_pwd = dbutils.secrets.get(scope="kv_name",key="pwd")

#These are the necessary parameters required to connect and run snowflake with Databricks.
options = {
  "sfUrl": "snowflake_url",
  "sfUser": non_prod_user,
  "sfPassword": non_prod_pwd,
  "sfDatabase":"database_name",
  "sfSchema": "schema_name",
  "sfWarehouse": "warehouse_name"
}

#write the dataframe to snowflake
#prerequisite: database,schema and table need to be present on snowflake

df.write.format("snowflake")\
.options(**options)\
.option("dbtable", "taxizone")\
.mode("append")\
.save()

