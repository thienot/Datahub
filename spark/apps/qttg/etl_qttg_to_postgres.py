"""
Spark Job: Silver layer (/opt/spark/data/lake/silver/master + /detail,
output của silver_qttg.py) -> transform -> ghi vào qttg_header / qttg_detail
(postgres-dwh), đảm bảo đúng khóa ngoại.

Chạy:
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/apps/qttg/etl_qttg_to_postgres.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Config - đường dẫn Silver layer local
MASTER_PATH = "/opt/spark/data/lake/silver/master"
DETAIL_PATH = "/opt/spark/data/lake/silver/detail"

# Config - Postgres đích (postgres-dwh, chạy trong cùng docker network)
PG_URL = "jdbc:postgresql://postgres-dwh:5432/qttg_dwh"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def main():
    spark = SparkSession.builder.appName("etl_qttg_to_postgres").getOrCreate()

    # Đọc Silver layer
    print(f"--- Đọc Master từ {MASTER_PATH} ---")
    df_master = spark.read.parquet(MASTER_PATH)

    print(f"--- Đọc Detail từ {DETAIL_PATH} ---")
    df_detail_raw = spark.read.parquet(DETAIL_PATH)

    total_master = df_master.count()
    total_detail_raw = df_detail_raw.count()
    print(f"--- Silver: master={total_master} dòng, detail={total_detail_raw} dòng ---")

    # ============================================
    # Transform Header
    # Dùng thẳng ID gốc từ file làm id trong bảng đích -> không cần
    # đọc lại DB để lấy id mới sinh, vì MASTER_ID bên Detail sẽ luôn khớp thẳng với ID này.
    # ============================================
    df_header_clean = (
        df_master
        .select(
            col("ID").alias("id"),
            col("NLD_ID").alias("nld_id"),
            col("SO_SO_BHXH").alias("so_so_bhxh"),
            col("THANG_BD").alias("thang_bd"),
            col("THANG_KT").alias("thang_kt"),
        )
    )

    header_clean_count = df_header_clean.count()
    print(f"--- Header sau transform: {header_clean_count} dòng ---")

    # Ghi Header vào postgres-dwh
    # (id được ghi TƯỜNG MINH -> Postgres chấp nhận vì id vẫn chỉ là BIGSERIAL
    df_header_clean.write.jdbc(
        url=PG_URL, table="qttg_header", mode="append", properties=PG_PROPS
    )
    print("--- Đã ghi xong qttg_header ---")

    # ============================================
    # Chọn cột cần cho Detail, đồng thời đổi luôn MASTER_ID -> header_id
    # vì header_id chính là ID gốc, không cần bước trung gian nào nữa.
    # ============================================
    df_detail_clean = df_detail_raw.select(
        col("MASTER_ID").alias("header_id"),
        col("MA_DON_VI").alias("ma_don_vi"),
        col("TEN_DON_VI").alias("ten_don_vi"),
        col("TU_THANG").alias("tu_thang"),
        col("DEN_THANG").alias("den_thang"),
        col("CHUC_DANH_CV").alias("chuc_danh"),
        col("NOI_LAM_VIEC").alias("noi_lam_viec"),
        col("MUC_LUONG").alias("muc_luong"),
    )

    # ============================================
    # INNER JOIN với Header đã ghi (theo header_id = id) để đảm bảo
    # Detail nào không khớp Header hợp lệ sẽ tự động bị loại,
    # không bao giờ ghi ra FK sai/mồ côi.
    # ============================================
    df_detail_final = df_detail_clean.join(
        df_header_clean.select(col("id").alias("header_id")),
        on="header_id", how="inner"
    )

    detail_final_count = df_detail_final.count()
    dropped = total_detail_raw - detail_final_count
    print(f"--- Detail sau join cuối cùng: {detail_final_count} dòng ---")
    if dropped > 0:
        print(f"--- CẢNH BÁO: {dropped} dòng Detail bị loại do không khớp được Header hợp lệ ---")

    # Ghi Detail vào postgres-dwh
    df_detail_final.write.jdbc(
        url=PG_URL, table="qttg_detail", mode="append", properties=PG_PROPS
    )
    print("--- Đã ghi xong qttg_detail ---")

    # Verify lại bằng cách đếm thật trong DB
    final_header = spark.read.jdbc(url=PG_URL, table="qttg_header", properties=PG_PROPS).count()
    final_detail = spark.read.jdbc(url=PG_URL, table="qttg_detail", properties=PG_PROPS).count()
    print(f"--- KẾT QUẢ CUỐI: qttg_header={final_header}, qttg_detail={final_detail} ---")

    spark.stop()


if __name__ == "__main__":
    main()