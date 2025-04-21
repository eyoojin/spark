# 0. RDD
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