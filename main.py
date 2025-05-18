import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, when

spark = SparkSession.builder.appName("hw-1").getOrCreate()
sc = spark.sparkContext

# 1.Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.

products_df = spark.read.csv("products.csv", header=True)
print('Products DataFrame:')
products_df.show()

purchases_df = spark.read.csv("purchases.csv", header=True)
print('Purchases DataFrame:')
purchases_df.show()

users_df = spark.read.csv('users.csv', header=True)
print('Users DataFrame:')
users_df.show()

# 2.Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
products_df = products_df.dropna()
print("Non nullable rows in products_df: ", products_df.count())

purchases_df = purchases_df.dropna()
print("Non nullable rows in purchases_df: ", purchases_df.count())

users_df = users_df.dropna()
print("Non nullable rows users_df: ", users_df.count())

# 3.Визначте загальну суму покупок за кожною категорією продуктів.

products_df.join(purchases_df, products_df.product_id == purchases_df.product_id, "right") \
    .select('category', 'price', 'quantity') \
    .dropna() \
    .withColumn('purchase_sum', col('quantity') * col('price')) \
    .groupBy('category') \
    .sum('purchase_sum').alias('purchase_sum') \
    .withColumn('purchase_sum', round('sum(purchase_sum)', 2)) \
    .drop('sum(purchase_sum)') \
    .show()

# 4.Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
purchases_df.join(products_df, products_df.product_id == purchases_df.product_id, "left") \
    .join(users_df, purchases_df.user_id == users_df.user_id, "left") \
    .select('category', 'price', 'quantity', 'age') \
    .filter(users_df.age.between(18, 25)) \
    .dropna() \
    .withColumn('purchase_sum', col('quantity') * col('price')) \
    .groupBy('category') \
    .sum('purchase_sum').alias('purchase_sum') \
    .withColumn('purchase_sum', round('sum(purchase_sum)', 2)) \
    .drop('sum(purchase_sum)') \
    .show()

# 5.Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
purchases_df.join(products_df, products_df.product_id == purchases_df.product_id, "left") \
    .join(users_df, purchases_df.user_id == users_df.user_id, "left") \
    .select('price', 'quantity', 'age', 'category') \
    .dropna() \
    .withColumn('total_sum', col('quantity') * col('price')) \
    .withColumn('target_sum', when(col('age').between(18, 25), col('quantity') * col('price')).otherwise(0)) \
    .groupBy('category') \
    .agg(
    sum(col('total_sum')).alias('total_sum'),
    sum(col('target_sum')).alias('target_sum'),
) \
    .withColumn('target_percentage', round(col('target_sum') / col('total_sum') * 100, 2)) \
    .select('category', 'target_percentage') \
    .show()

# 6.Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.

sum_by_categories_df = purchases_df.join(products_df, products_df.product_id == purchases_df.product_id, "left") \
    .join(users_df, purchases_df.user_id == users_df.user_id, "left") \
    .select('category', 'price', 'quantity', 'age') \
    .filter(users_df.age.between(18, 25)) \
    .dropna() \
    .withColumn('purchase_sum', col('quantity') * col('price')) \
    .groupBy('category') \
    .sum('purchase_sum').alias('purchase_sum')

total_sum = sum_by_categories_df.groupBy().sum().collect()[0][0]

sum_by_categories_df.withColumn('percentage', round(col('sum(purchase_sum)') / total_sum * 100, 2)) \
    .select('category', 'percentage') \
    .orderBy(col('percentage').desc()) \
    .limit(3) \
    .show()

spark.stop()
