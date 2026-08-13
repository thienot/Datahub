import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold_qttg").getOrCreate()

    detail_df = spark.read.parquet(f"{args.input_dir}/detail")

    
    gold_df = (
        detail_df.groupBy("THANG").agg(
        count_distinct("NLD_ID").alias("SO_NGUOI_THAM_GIA"),
        count_distinct("MA_DON_VI").alias("SO_DON_VI_THAM_GIA"),
        sum("MUC_LUONG").alias("TONG_QUY_LUONG"),
        avg(
            when(col("MUC_LUONG") > 0, col("MUC_LUONG"))
        ).alias("LUONG_BINH_QUAN"),
        count_distinct(
            when(col("MUC_LUONG") == 0, col("NLD_ID"))
        ).alias("SO_NGUOI_CO_MUC_LUONG_0")
    ).orderBy(desc(col("THANG")))
    )

    gold_df.write.mode("overwrite").parquet(args.output_dir)

    report_count = gold_df.count()

    print(f"LAYER=GOLD STATUS=SUCCESS REPORT_ROWS={report_count}")
    gold_df.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()