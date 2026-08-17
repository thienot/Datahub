import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def validate_month_range(df, layer_name):
    """
    Validate TU_THANG và DEN_THANG:
    1. Không được NULL / empty.
    2. Đúng định dạng YYYYMM.
    3. Tháng nằm trong khoảng 01-12.
    4. TU_THANG <= DEN_THANG.
    """

    invalid_count = df.filter(
        # 1.null/empty
        col("TU_THANG").isNull()
        | (trim(col("TU_THANG")) == "")
        | col("DEN_THANG").isNull()
        | (trim(col("DEN_THANG")) == "")
        # 2.format YYYYMM
        | (~trim(col("TU_THANG")).rlike(r"^[0-9]{6}$"))
        | (~trim(col("DEN_THANG")).rlike(r"^[0-9]{6}$"))
        # 3.month 01-12
        | (~substring(trim(col("TU_THANG")), 5, 2).between("01", "12"))
        | (~substring(trim(col("DEN_THANG")), 5, 2).between("01", "12"))
        # 4.TU_THANG <= DEN_THANG
        | (trim(col("TU_THANG"))> trim(col("DEN_THANG"))
        )
    ).count()

    print(
        f"{layer_name.upper()}_INVALID_MONTH_RANGE_ROWS="
        f"{invalid_count}"
    )

    return invalid_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("validate_qttg").getOrCreate()

    silver_master = spark.read.parquet(f"{args.lake_dir}/silver/master")
    silver_detail = spark.read.parquet(f"{args.lake_dir}/silver/detail")
    gold_df = spark.read.parquet(f"{args.lake_dir}/gold")

    errors = []

    # 1) Detail không được mồ côi (mọi MASTER_ID phải tồn tại trong silver_master)
    master_ids = silver_master.select(col("ID").alias("MASTER_ID"))
    orphan_detail_count = (
        silver_detail.join(master_ids, on="MASTER_ID", how="left_anti").count()
    )
    if orphan_detail_count > 0:
        errors.append(f"orphan_detail_rows={orphan_detail_count}")

    # 2) Mỗi người chỉ còn đúng một master tại Silver
    dup_person_count = silver_master.select("SO_SO_BHXH").groupBy("SO_SO_BHXH").agg(count("*").alias("total")).where(col("total") != 1).count()
    if dup_person_count > 0:
        errors.append(f"duplicate_person_rows={dup_person_count}")

    # 3) Kiểm tra bản ghi có thông tin tháng không hợp lệ
    master_invalid_month_count = validate_month_range(silver_master,"master")
    if master_invalid_month_count > 0:
        errors.append(f"master_invalid_month_range_rows={master_invalid_month_count}")

    detail_invalid_month_count = validate_month_range(silver_detail,"detail")
    if detail_invalid_month_count > 0:
        errors.append(f"detail_invalid_month_range_rows={detail_invalid_month_count}")

    # 4) Gold không trùng tháng
    dup_month_count = (
        gold_df.groupBy("THANG")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    if dup_month_count > 0:
        errors.append(f"duplicate_gold_month_rows={dup_month_count}")

    if errors:
        print(f"LAYER=VALIDATE STATUS=FAILED ERROR_ROWS={len(errors)} DETAIL={errors}")
        spark.stop()
        # Bắt buộc raise/exit khác 0 để Airflow đánh dấu task thất bại
        sys.exit(1)

    print("LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0")
    spark.stop()


if __name__ == "__main__":
    main()