# VISHAL: List Of Databases for the table.

#get_databricks_count = spark.sql(f"select count(*) from b2b_staging.{table_name}").collect()
database_name = spark.sql("show databases").collect()
# print(table_name)
table = 'bill'
l1 = []
final = {}
# get the database names
# for x in database_name:
#   l1.append(x.databaseName)
l1 = [x.databaseName for x in database_name]
# add tables to the dictionary 
for x in l1:
  tables = spark.sql("show tables in "+x).collect()
  final[x] = [x.tableName for x in tables]
# print(len(final['b2b_staging']))
# print(final['b2b_staging'])

# databases containing the table
# l2 = [x for x in l1 if table in final[x]]
# print(l2)
print([x for x in l1 if table in final[x]])