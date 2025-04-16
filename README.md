# pyenv
```shell
pyenv global 3.11.12
```

# mockaroo
- 랜덤하게 data를 만들 수 있는 사이트

# pyspark
```shell
pip install pyspark
```
# RDD
- wordcount
```python
%pyspark

# sc = SparkContext()

# 로컬 파일 읽기
file_path = 'file:///home/ubuntu/damf2/data/word.txt'
lines = sc.textFile(file_path)
print(lines.collect())

# HDFS에서 파일 읽기 (HDFS 업로드 후)
file_path = 'hdfs://localhost:9000/input/word.txt'
lines = sc.textFile(file_path)

# 맵 리듀스
words = lines.flatMap(lambda line: line.split()) # 데이터 쪼개기 = 평탄화

mapped_words = words.map(lambda word: (word, 1))

reduced_words = mapped_words.reduceByKey(lambda a, b: a+b) # 키를 기준으로 리듀스
```

- log
```python
%pyspark

file_path = 'file:///home/ubuntu/damf2/data/logs/2024-01-01.log'
lines = sc.textFile(file_path)

mapped_lines = lines.map(lambda line: line.split())

# 4xx status code filtering
def filter_4xx(line):
    return line[5][0] == '4'
    
filtered_lines = mapped_lines.filter(filter_4xx)

#  method('GET', 'POST')별 요청 수 계산
method_rdd = mapped_lines.map(lambda line: (line[2], 1)).reduceByKey(lambda a, b: a+b)

# 시간대별 요청 수
time_rdd = mapped_lines.map(lambda line: (line[1].split(':')[1], 1)).reduceByKey(lambda a, b: a+b)

# status_code, api method별 count
count_rdd = mapped_lines.map(lambda line: ((line[5], line[2]), 1)).reduceByKey(lambda a, b: a+b)
```

- join
```python
%pyspark

user_file_path = 'file:///home/ubuntu/damf2/data/user.csv'
post_file_path = 'file:///home/ubuntu/damf2/data/post.csv'

user_lines = sc.textFile(user_file_path)
post_lines = sc.textFile(post_file_path)

user_rdd = user_lines.map(lambda line: line.split(','))
post_rdd = post_lines.map(lambda line: line.split(','))

user_tuple = user_rdd.map(lambda user: (user[0], user)) # (user_id, user)
post_tuple = post_rdd.map(lambda post: (post[1], post)) # (user_id, post)

joined_rdd = user_tuple.join(post_tuple)
```

# DataFrame
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