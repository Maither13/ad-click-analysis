from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count
import pymysql

# ===================== 1. 配置常量（统一管理）=====================
# Kafka配置
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "ad_click"
STARTING_OFFSETS = "latest"

# MySQL配置
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"
MYSQL_DB = "davinci"

# 流式配置
BATCH_INTERVAL = "10 seconds"
# 成功日志开关，False只打印错误，避免刷屏
PRINT_SUCCESS_LOG = False


# ===================== 2. MySQL批量写入工具函数 =====================
def upsert_mysql(partition, table_name, pk_col, value_col="click_count"):
    """
    批量upsert写入MySQL，性能优于逐行插入
    :param partition: spark分区数据
    :param table_name: 目标表名
    :param pk_col: 主键字段名
    :param value_col: 统计值字段名
    """
    conn = None
    data_list = []

    try:
        # 收集分区数据
        for row in partition:
            data_list.append((row[pk_col], row[value_col], row[value_col]))

        # 空分区跳过
        if not data_list:
            return

        # 建立连接
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DB,
            charset="utf8"
        )
        cursor = conn.cursor()

        # 批量执行upsert：存在则累加，不存在则插入
        sql = f"""
            INSERT INTO {table_name} ({pk_col}, {value_col})
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE {value_col} = {value_col} + %s
        """
        cursor.executemany(sql, data_list)
        conn.commit()

        if PRINT_SUCCESS_LOG:
            print(f"[成功] 表 {table_name} 写入 {len(data_list)} 条数据")

    except Exception as e:
        print(f"[失败] 表 {table_name} 写入异常")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误详情: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            cursor.close()
            conn.close()


# ===================== 3. 创建SparkSession（性能优化）=====================
spark = SparkSession.builder \
    .appName("AdClickRealTime") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
# 过滤Spark冗余INFO日志
spark.sparkContext.setLogLevel("WARN")

# ===================== 4. 读取Kafka并解析日志 =====================
# 读取Kafka流
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .load()

# 解析日志格式：广告ID,省份,城市,用户ID
lines = df.selectExpr("CAST(value AS STRING) as line")
split_cols = split(col("line"), ",")
click_df = lines.select(
    split_cols.getItem(0).alias("ad_id"),
    split_cols.getItem(1).alias("province"),
    split_cols.getItem(2).alias("city"),
    split_cols.getItem(3).alias("user_id")
)

# ===================== 5. 三个核心维度聚合计算 =====================
# 维度1：广告总点击量
ad_click_count = click_df.groupBy("ad_id").agg(count("*").alias("click_count"))

# 维度2：省份点击量
province_count = click_df.groupBy("province").agg(count("*").alias("click_count"))

# 维度3：城市点击量
city_count = click_df.groupBy("city").agg(count("*").alias("click_count"))

# ===================== 6. 启动三个流式写入 =====================
# 1. 广告点击写入MySQL
query_ad = ad_click_count.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, _: df.foreachPartition(
    lambda p: upsert_mysql(p, "ad_click_top", "ad_id")
)) \
    .trigger(processingTime=BATCH_INTERVAL) \
    .start()

# 2. 省份点击写入MySQL
query_province = province_count.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, _: df.foreachPartition(
    lambda p: upsert_mysql(p, "province_click_top", "province")
)) \
    .trigger(processingTime=BATCH_INTERVAL) \
    .start()

# 3. 城市点击写入MySQL
query_city = city_count.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, _: df.foreachPartition(
    lambda p: upsert_mysql(p, "city_click_top", "city")
)) \
    .trigger(processingTime=BATCH_INTERVAL) \
    .start()

# ===================== 7. 阻塞主线程，持续运行 =====================
spark.streams.awaitAnyTermination()
