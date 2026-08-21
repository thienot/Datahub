"""
Spark Job: Silver layer (/opt/spark/data/lake/silver/master + /detail,
output của silver_qttg.py) -> transform -> ghi vào qttg_header / qttg_detail
(postgres-dwh), đảm bảo đúng khóa ngoại.

Chạy:
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/apps/qttg/etl_qttg_to_postgres.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import uuid

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

def add_random_full_name(df: DataFrame) -> DataFrame:
    """
    Thêm cột full_name random vào DataFrame
    """
    ho = array(
        lit("Nguyễn"), lit("Trần"), lit("Lê"), lit("Phạm"),
        lit("Hoàng"), lit("Huỳnh"), lit("Vũ"), lit("Võ")
    )
    ten_dem = array(
        lit("Văn"), lit("Thị"), lit("Đức"), lit("Minh"),
        lit("Thành"), lit("Hữu"), lit("Quốc")
    )
    ten = array(
        lit("An"), lit("Bình"), lit("Cường"), lit("Dũng"),
        lit("Hùng"), lit("Nam"), lit("Phong"), lit("Tuấn"),
        lit("Hà"), lit("Lan"), lit("Thiện"), lit("Anh"), lit("Huy"),
        lit("Minh"), lit("Thắng"), lit("Trung"), lit("Tài"), lit("Quang"),
        lit("Trang"), lit("Đức"), lit("Mạnh"), lit("Thịnh"), lit("Thái"),
        lit("Đạt"),lit("Bắc"),lit("Linh"),lit("Huệ"),lit("Hồng"),lit("Trà"),
    )

    #concat: nối thành 1 chuỗi hoàn chỉnh
    #element_at: lấy vị trí chỉ số trong mảng (Spark lấy chỉ số từ số 1)
    #floor: lấy phần nguyên bỏ phần thập phân
    return df.withColumn(
        "full_name",
        concat(
            element_at(ho, floor(rand() * 8 + 1).cast("int")),
            lit(" "),
            element_at(ten_dem, floor(rand() * 7 + 1).cast("int")),
            lit(" "),
            element_at(ten, floor(rand() * 29 + 1).cast("int"))
        )
    )

#hàm format lại ngày từ dạng YYYYMM → MM/YYYY
def format_yyyymm_to_mmyyyy(df: DataFrame, col_name: str) -> DataFrame:
    """
    Format cột từ dạng 199204 → 04/1992
    """
    return df.withColumn(
        col_name,
        concat(
            substring(col(col_name).cast("string"), 5, 2),  # lấy MM
            lit("/"),
            substring(col(col_name).cast("string"), 1, 4)   # lấy YYYY
        )
    )

def main():
    spark = SparkSession.builder.appName("etl_qttg_to_postgres").getOrCreate()

    #1. Tạo batch_id duy nhất cho lần chạy này
    vn_time = datetime.utcnow() + timedelta(hours=7)
    batch_id = vn_time.strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]    
    print(f"--- Batch ID của lần chạy này: {batch_id} ---")

    # Đọc Silver layer
    print(f"--- Đọc Master từ {MASTER_PATH} ---")
    df_master = spark.read.parquet(MASTER_PATH)

    print(f"--- Đọc Detail từ {DETAIL_PATH} ---")
    df_detail_raw = spark.read.parquet(DETAIL_PATH)

    total_master = df_master.count()
    total_detail_raw = df_detail_raw.count()
    print(f"--- Silver: master={total_master} dòng, detail={total_detail_raw} dòng ---")

    # Transform Header
    # Dùng thẳng ID gốc từ file làm id trong bảng đích -> không cần đọc lại DB để lấy id mới sinh, 
    # vì MASTER_ID bên Detail sẽ luôn khớp thẳng với ID này.
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
    #Thêm random cho trường full name để phục vụ cho bài tập search theo name
    df_header_clean = add_random_full_name(df_header_clean)

    #Sửa lại format theo đúng output yêu cầu là MM/yyyy
    df_header_clean = format_yyyymm_to_mmyyyy(df_header_clean, "thang_bd")
    df_header_clean = format_yyyymm_to_mmyyyy(df_header_clean, "thang_kt")

    #Thêm các cột Audit/Log cho master
    df_header_clean = (
        df_header_clean
        .withColumn("created_by", lit("spark-job"))
        .withColumn("updated_by", lit("spark-job"))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("is_deleted", lit(False))
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
    df_detail_clean = format_yyyymm_to_mmyyyy(df_detail_clean, "tu_thang")
    df_detail_clean = format_yyyymm_to_mmyyyy(df_detail_clean, "den_thang")
    #Thêm các cột Audit/Log cho master
    df_detail_clean = (
            df_detail_clean
            .withColumn("created_by", lit("spark-job"))
            .withColumn("updated_by", lit("spark-job"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("is_deleted", lit(False))
        )

    detail_final_count = df_detail_clean.count()
    dropped = total_detail_raw - detail_final_count
    print(f"--- Detail sau join cuối cùng: {detail_final_count} dòng ---")
    if dropped > 0:
        print(f"--- CẢNH BÁO: {dropped} dòng Detail bị loại do không khớp được Header hợp lệ ---")

    # Ghi Detail vào postgres-dwh
    df_detail_clean.write.jdbc(
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