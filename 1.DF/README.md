# 1. DataFrame
```python
%pyspark

# load data
file_path = 'file:///home/ubuntu/damf2/data/logs/2024-01-01.log'

df = spark.read.csv(file_path, sep=' ')
df.show()

# show
df.show(1)

# columns
df.columns

# schema
df.printSchema()

# select columns
df.select('_c0', '_c1').show()

# head
df.take(2)
```

- Pandas
```python
%pyspark

# toPandas
pd_df = df.toPandas()

# Pandas Dataframe columns
pd_df[['_c0', '_c2']]
```

- spark DataFrame
```python
%pyspark

# columns
df.select(df._c2).show()

# 새로운 컬럼 추가
from pyspark.sql.functions import split, col
df = df.withColumn('method', split(col('_c2'), ' ').getItem(0))
df = df.withColumn('path', split(col('_c2'), ' ').getItem(1))
df = df.withColumn('protocal', split(col('_c2'), ' ').getItem(2))

# filter
df.filter(df.method == 'POST').show()

# groupby
df.groupby('method').count().show()

# aggregation
from pyspark.sql.functions import min, max, mean
df = df.select('method', '_c3', col('_c4').cast('integer'))
df.groupby('method', '_c3').agg(min('_c4'), max('_c4'), mean('_c4')).show()
```

- spark sql
```python
%pyspark

file_path = 'file:///home/ubuntu/damf2/data/logs/2024-01-01.log'
df = spark.read.csv(file_path, sep=' ')
df.createOrReplaceTempView('logs')

# select
spark.sql('''
    SELECT * FROM logs
''').show()

# add column
df = spark.sql('''
    SELECT *, SPLIT(_c2, ' ')[0] AS method, SPLIT(_c2, ' ')[1] AS path, SPLIT(_c2, ' ')[2] as protocal
    FROM logs
''')
df.show()
df.createOrReplaceTempView('logs2')

# where
spark.sql('''
    SELECT * FROM logs2
    WHERE _c3 = 400
''').show()

# and
spark.sql('''
    SELECT * FROM logs2
    WHERE _c3 = 200 AND path LIKE '%product%'
''').show()

# group by
spark.sql('''
    SELECT method, COUNT(*) FROM logs2
    GROUP BY method
''').show()
```