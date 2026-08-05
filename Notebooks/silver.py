%md
## DATA ACCESS USING APP

from pyspark.sql.functions import *
from pyspark.sql.types import *
spark.conf.set(
    "fs.azure.account.auth.type.<storage_account>.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.<storage_account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.<storage_account>.dfs.core.windows.net",
    "<client_id>"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.secret.<storage_account>.dfs.core.windows.net",
    "client_secret"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.<storage_account>.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant_id>/oauth2/token"
)
%md
## DATA LOADING
%md
#### Reading data
df_cal = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Calendar')
df_cus = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Customers')
df_procat = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Product_Categories')
df_pro = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Products')
df_ret = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Returns')
df_sales = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Sales_*')
df_ter = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/AdventureWorks_Territories')
df_subcat = spark.read.\
    format('csv').\
    option('inferSchema',True).\
    option('header',True).\
    load('abfss://bronze@<storage_account>.dfs.core.windows.net/Product_Subcategories')
%md
##TRANSFORMATIONS
%md
####Calendar
df_cal.display()
df_cal=df_cal.withColumn('Month',month(col('Date')))\
    .withColumn('Year',year(col('Date')))
df_cal.write.format('parquet')\
    .mode('append')\
    .option('path','abfss://silver@<storage_account>.dfs.core.windows.net/Adventureworks_Calender')\
    .save()
%md
####Customers
df_cus=df_cus.withColumn('FullName',concat_ws(' ',col('Prefix'), col('FirstName'), col('LastName')))
df_cus.write.format('parquet')\
    .mode('append')\
    .option('path','abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_customers')\
    .save()

%md
####Product Categories
df_procat.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_Product_Categories')\
    .save()
%md
####Products
df_pro=df_pro.withColumn('ProductSKU', split(col('ProductSKU'),'-')[0])\
    .withColumn('ProductName', split(col('ProductName'),' ')[0])
df_pro.write\
    .format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_Products')\
    .save()
%md
####Returns
df_ret.write\
    .format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_Returns')\
    .save()
%md
####Territories
df_ter.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_Territories')\
    .save()
%md
####Sub-categoeies
df_subcat.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_Subcategories')\
    .save()
%md
####Sales
df_sales=df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))\
    .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
    .withColumn('Multiply', col('OrderLineItem')* col("OrderQuantity"))
df_sales.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@<storage_account>.dfs.core.windows.net/Adventure_sales')\
    .save() 
%md
####Analysis
# df_sales.display()
df_sales.groupBy(col('OrderDate')).agg(count(col('OrderNumber')).alias('count')).display()
df_pro.groupBy(col('ProductSKU')).agg(round(avg(col('ProductCost'))).alias('avg_cost')).display()
df_pro.select(countDistinct('ProductSubCategoryKey').alias('Total Product Subcategories')).display()
df_cus.select('CustomerKey' ,'HomeOwner', 'AnnualIncome').display()
df_cus.groupBy(col('HomeOwner'))\
    .agg(count('*').alias('Home Owning Customer Count'))\
    .display()
df_ret.groupBy("TerritoryKey").agg(sum("ReturnQuantity").alias('Total Returns')).display()

