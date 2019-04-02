# Databricks notebook source
#Table 삭제
spark.sql("""
DROP TABLE flights_json """)
spark.sql("""
DROP TABLE flight_from_select""")
spark.sql("""
DROP TABLE partitioned_flights
""")
spark.sql("""
DROP TABLE hive_flights
""")

##Table 생성
spark.sql("""
CREATE TABLE flights_json (DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING COMMENT "This is comment",
count LONG)
USING JSON OPTIONS (path "/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")
""")

#SELECT를 이용한 Table 생성
spark.sql("""
CREATE TABLE flight_from_select USING parquet AS SELECT * FROM flights_json
""")

#파티셔닝된 데이터셋을 저장해 데이터 레이아웃을 제어
spark.sql("""
CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5
""")

#외부 테이블 생성
spark.sql("""
CREATE EXTERNAL TABLE hive_flights (
DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/databricks-datasets/definitive-guide/data/flight-data-hive'
""")

#테이블에 데이터 삽입 
spark.sql("""
INSERT INTO flight_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20""")

#특정 파티션에만 데이터를 추가하고 싶을 때
spark.sql("""
INSERT INTO partitioned_flights
PARTITION (DEST_COUNTRY_NAME = "UNITED STATES")
SELECT count, ORIGIN_COUNTRY_NAME FROM flights
WHERE DEST_COUNTRY_NAME = 'UNITED STATES' LIMIT 12
""")

##메타데이터 확인
#comment 확인
spark.sql("""
DESCRIBE TABLE flights_json""")

#스키마 정보 확인
spark.sql("""
SHOW PARTITIONS partitioned_flights
""")

##메타데이터 갱신
#REFRESH TABLE : 테이블과 관련된 모든 캐싱된 항목을 갱신
spark.sql("""
REFRESH TABLE partitioned_flights
""")

#REPAIR TABLE : 새로운 파티션 정보를 수집
spark.sql("""
MSCK REPAIR TABLE partitioned_flights
""")

#Table 캐싱
spark.sql("""
CACHE TABLE flights
""")

### View 
#View를 사용해서 신규 경로에 모든 데이터를 다시 저장하는 대신 쿼리 시점의 데이터소스에서 트랜스포메이션을 수행
#목적지 : United states인 데이터를 보기위한 뷰를 생성
#임시 뷰
spark.sql("""
CREATE TEMP VIEW just_usa_temp_view AS
SELECT * FROM flights WHERE DEST_COUNTRY_NAME = 'United States'
""")
#전역 뷰
spark.sql("""
CREATE GLOBAL VIEW just_usa_global_view AS
SELECT * FROM flights WHERE DEST_COUNTRY_NAME = 'United States'
""")

#덮어쓰기
spark.sql("""
CREATE OR REPLACE STEMP VIEW just_usa_temp_view AS
SELECT * FROM flights WHERE DEST_COUNTRY_NAME = 'United States'
""")

"""
쿼리가 실행될 때만 뷰에 정의된 트랜스포메이션을 수행
"""

##실행계획 확인
spark.sql("""
EXPLAIN SELECT * FROM just_usa_view
""")


# COMMAND ----------

###Database 
spark.sql("""
SHOW DATABASES""")

spark.sql("""
CREATE DATABASE some_db
""")
spark.sql("""
USE some_db
""")
spark.sql("""
SHOW TABLES
SELECT * FROM flights""") ##Error : No table or view

#다른 db의 table이나 view 사용
spark.sql("""
SHOW TABLES
SELECT * FROM defalut.flights
""")

#현재 DB 확인
spark.sql("""
SELECT current_database()
""")

#SELECT CASE .. WHEN .. THEN...END
spark.sql("""
SELECT
  CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
  CASE WHEN DEST_COUNTRY_NAME = 'EGYPT' THEN 0
  ELSE -1 END
FROM partitioned_flights
""")

# COMMAND ----------

##구조체
spark.sql("""
CREATE VIEW IF NOT EXISTS nested_data AS
SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) AS country, count FROM flight
""")

#구조체 개별 컬럼 조회
spark.sql("""
SELECT country.DEST_COUNTRY_NAME, count FROM nested_data
""")

##리스트
#생성
spark.sql("""
SELECT DEST_COUNTRY_NAME AS new_name, collect_list(count) AS flight_counts,
  collect_set(ORIGIN_COUNTRY_NAME) AS origin_set
FROM flight GROUP BY DEST_COUNTRY_NAME
""")
#분해
spark.sql("""
SELECT explode(flight_counts),DEST_COUNTRY_NAME FROM flights_agg
""")

#함수 확인 : SHOW FUNCTIONS
#서브 쿼리
spark.sql("""
SELECT * FROM flights
WHERE ORIGIN_COUNTRY_NAME IN (SELECT DEST_COUNTRY_NAME FROM flights
      GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC LIMIT 5)
""")

