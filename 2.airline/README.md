# 2. airline
- data upload
```sh
# damf2/data/airline
for year in $(seq 2007 2008)
do
    hdfs dfs -put $year.csv /input/airline
done
```
- 데이터 불러오기
```python
%pyspark

file_path = 'hdfs://localhost:9000/input/airline'

df = spark.read.csv(file_path, header=True, inferSchema=True)

# 데이터 타입 확인
df.printSchema()

# 데이터 개수 확인
df.count()

# 데이터 예쁘게 보기
z.show(df)

# 일부 컬럼만 추출 + 형변환
from pyspark.sql.functions import *
df = df.select(
    'Month',
    'DayofMonth',
    'DayOfWeek',
    'Orgin',
    'Dest',
    'Cancelled',
    'UniqueCarrier',
    'Distnace',
    col('AirTime').cast('int'),
    col('ArrTime').cast('int'),
    col('ArrDelay').cast('int'),
    col('DepTime').cast('int'),
    col('DepDelay').cast('int'),
    col('ActualElapsedTime').cast('int'),
    col('CRSElapsedTime').cast('int'),
)

# sql 테이블 생성
df.createOrReplaceTempView('airline')
```
## sql vs pyspark
- 10개 행 추출
```sql
%sql
SELECT * FROM airline LIMIT 10;
```
```python
%pyspark
z.show(df.limit(10))
```

- 전체 데이터 개수 세기
```sql
%sql
SELECT COUNT(*) FROM airline;
```
```python
%pyspark
df.count()
```

- 중복 없이 항공사 코드 추출
```sql
%sql
SELECT DISTINCT UniqueCarrier
FROM airline
ORDER BY UniqueCarrier;
```
```python
%pyspark
df.select('UniqueCarrier').distinct().orderBy('UniqueCarrier').show()
```

- 항공사별 운항 건수
```sql
%sql
SELECT UniqueCarrier, COUNT(*)
FROM airline
GROUP BY UniqueCarrier;
```
```python
%pyspark
df.groupBy('UniqueCarrier').agg(count('*')).show()
# agg: 새로운 컬럼을 만들어서 계산 결과 출력
```

- 요일별 출발/도착 지연 평균
```sql
%sql
SELECT DayOfWeek, AVG(DepDelay), AVG(ArrDelay)
FROM airline
GROUP BY DayOfWeek;
```
```python
%pyspark
df.groupBy('DayOfWeek').agg(
    mean('DepDelay'), 
    mean('ArrDelay')
    ).show()
```

- 항공사별, 월별 지연 및 운항 건수
```sql
%sql
SELECT
    UniqueCarrier,
    Month,
    SUM(CASE WHEN DepDelay > 0 THEN 1 ELSE 0 END) AS depdelay_count,
    SUM(CASE WHEN ArrDelay > 0 THEN 1 ELSE 0 END) AS arrdelay_count,
    COUNT(*) AS total_flight
FROM airline
GROUP BY UniqueCarrier, Month
ORDER BY UniqueCarrier, Month;
```
```sql
%sql
SELECT
    UniqueCarrier,
    Month,
    AVG(DepDelay),
    AVG(ArrDelay),
    COUNT(*)
FROM airline
GROUP BY UniqueCarrier, Month
ORDER BY UniqueCarrier, Month;
```
```python
%pyspark
df.groupBy('UniqueCarrier', 'Month').agg(
    count(when(col('DepDelay') > 0, col('DepDelay'))).alias('depdelay_count'), 
    count(when(col('ArrDelay') > 0, col('ArrDelay'))).alias('arrdelay_count'), 
    count('*').alias('total_flight')
    ).orderBy('UniqueCarrier', 'Month').show()
```
```python
%pyspark
df.groupBy('UniqueCarrier', 'Month').agg(
    avg('DepDelay'),
    avg('ArrDelay'),
    count('*')
).orderBy('UniqueCarrier', 'Month').show()
```

- 항공사별 취소율
```sql
%sql
SELECT 
    UniqueCarrier,
    SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*) AS cancellation_rate
FROM airline
GROUP BY UniqueCarrier;
```
```sql
%sql
SELECT 
    UniqueCarrier,
    (SUM(Cancelled) / COUNT(*) * 100) AS cancellation_rate
FROM airline
GROUP BY UniqueCarrier;
```
```sql
%sql
SELECT 
    *,
    (cancelled_count / total_count * 100) AS cancellation_rate
FROM
(SELECT
    UniqueCarrier,
    SUM(Cancelled) AS cancelled_count,
    COUNT(*) AS total_count
FROM airline
GROUP BY UniqueCarrier);
```
```python
%pyspark
df.groupBy('UniqueCarrier').agg(
    (count(when(df.Cancelled == 1, df.Cancelled))/count('*')).alias('cancelled_rate')
).show()
```
```python
%pyspark
df.groupBy('UniqueCarrier').agg(
    (sum(df.Cancelled) / count('*') * 100).alias('cancelled_rate')
).show()
```
```python
%pyspark
df.groupBy('UniqueCarrier').agg(
    sum(df.Cancelled).alias('cancelled_count'),
    count('*').alias('total_count')
).withColumn('cancellation_rate', col('cancelled_count')/col('total_count')*100).show()
```

- 가장 붐비는 공항
```sql
%sql
SELECT *, origin_count + dest_count AS total_count
FROM (
(SELECT Origin, COUNT(*) AS origin_count
FROM airline
GROUP BY Origin) AS origin_airline
JOIN
(SELECT Dest, COUNT(*) AS dest_count
FROM airline
GROUP BY Dest) AS dest_airline
ON origin_airline.Origin = dest_airline.Dest
)
ORDER BY total_count DESC;
```
```python
%pyspark
origin_df = df.groupBy('Origin').count()
dest_df = df.groupBy('Dest').count()

origin_df.join(
    dest_df, 
    origin_df.Origin == dest_df.Dest
).withColumn(
    'total_count', 
    origin_df['count'] + dest_df['count']
).orderBy(desc('total_count')).show()
```

- 실제 비행시간/ 예상 비행시간 차이가 큰 비행
```sql
%sql
SELECT
    Month,
    DayofMonth,
    Origin,
    Dest,
    ABS(ActualElapsedTime - CRSElapsedTime) AS diff_time
FROM airline
ORDER BY diff_time DESC;
```
```sql
%sql
SELECT
    *,
    ABS(real_time - crs_time) AS diff_time
FROM
(SELECT
    Origin,
    Dest,
    AVG(ActualElapsedTime) AS real_time,
    AVG(CRSElapsedTime) AS crs_time
FROM airline
GROUP BY Origin, Dest)
ORDER BY diff_time DESC;
```
```python
%pyspark
df.select(
    'Month', 
    'DayofMonth', 
    'Origin', 
    'Dest', 
    abs(col('ActualElapsedTime')-col('CRSElapsedTime')).alias('diff_time')
).orderBy(desc('diff_time')).show()
```
```python
%pyspark
df.groupBy('Origin', 'Dest').agg(
    avg('ActualElapsedTime').alias('real_time'),
    avg('CRSElapsedTime').alias('crs_time')
).withColumn(
    'diff_time', 
    abs(col('real_time')-col('crs_time'))
).orderBy(desc('diff_time')).show()
```