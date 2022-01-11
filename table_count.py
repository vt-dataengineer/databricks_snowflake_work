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

# comparing the row counts in snowflake and databricks
def match_count(table_name):
  get_snowflake_count = spark.read.format("snowflake").options(**options).option("query","select count(*) from "+table_name).load().collect()
  get_databricks_count = spark.sql(f"select count(*) from database_name.{table_name}").collect()
  print(get_snowflake_count)
  print(get_databricks_count)
  # split list content
  s11 = str(get_snowflake_count).split('=')[1]
  s12 = str(get_databricks_count).split('=')[1]
  # slice the string to get the count
  ss = s11[s11.find('(')+1:s11.find(')')]
  # convert the string values to integer
  snowflake_count = int(ss[1:-1])
  databricks_count = int(s12[:s12.find(')')])
  print('snowflake count: ',snowflake_count)
  print('databricks count: ',databricks_count)
  if snowflake_count == databricks_count:
    print('Data count matched')
  else:
    print('Data count mismacthed')
match_count("bill")