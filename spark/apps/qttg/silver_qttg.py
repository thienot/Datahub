import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_qttg").getOrCreate()

    master_df = spark.read.parquet(f"{args.input_dir}/master")
    detail_df = spark.read.parquet(f"{args.input_dir}/detail")


    # Với mỗi SO_SO_BHXH, chọn bản ghi master mới nhất theo CREATED_AT DESC, ID DESC
    window = Window.partitionBy("SO_SO_BHXH").orderBy(desc("CREATED_AT"),desc("ID"))

    latest_master_df = (
        master_df
        .withColumn("rank", row_number().over(window)) 
        .filter(col("rank") == 1)
        .drop("rank")
    )

    # Chỉ giữ detail thuộc về các master vừa chọn (loại bỏ detail mồ côi)
    latest_detail_df = latest_master_df.select("ID") \
    .join(detail_df, latest_master_df.ID == detail_df.MASTER_ID, "left").drop(latest_master_df.ID)

    #Xử lý dữ liệu để Gold sử dụng để đưa ra kết quả
    #Cách làm: TU_THANG - DEN_THANG
    #Vì cần từng tháng của các năm nên cần xử lý để lấy các tháng trong khoảng cột "TU_THANG" đến cột "DEN_THANG"
    # 2 cột đó đang là string nên phải chuyển về format về dạng date dùng to_date() (ví dụ 202609 thành 2026-09-01 )
    # sau đó tiếp tục tạo danh sách "DS_THANG" tháng "TU_THANG" - "COT_THANG",ví dụ: từ 1998-12-01  đến 1999-02-01 thành 1998-12-01,1999-01-01,1999-02-01 bằng hàm sequence
    # tiếp đến tách danh sách đó mỗi tháng thành một hàng (row) dùng hàm explose()
    #cuối cùng là chuyển về định dạng theo yêu cầu yyyyMM
    latest_detail_df = latest_detail_df\
    .withColumn("TU_THANG_DATE", to_date(col("TU_THANG"), 'yyyyMM'))\
    .withColumn("DEN_THANG_DATE", to_date(col("DEN_THANG"), 'yyyyMM'))\
    .withColumn("DS_THANG", sequence(col("TU_THANG_DATE"), col("DEN_THANG_DATE"), expr("INTERVAL 1 MONTH"))) \
    .withColumn("THANG", explode(col("DS_THANG"))) \
    .withColumn("THANG", date_format(col("THANG"), "yyyyMM")) \
    .drop("TU_THANG_DATE", "DEN_THANG_DATE","DS_THANG") \
    .orderBy(desc(col("NLD_ID")), asc(col("THANG")))

    latest_master_df.write.mode("overwrite").parquet(f"{args.output_dir}/master")
    latest_detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/detail")

    person_count = latest_master_df.count()
    detail_count = latest_detail_df.count()

    print(
        f"LAYER=SILVER STATUS=SUCCESS "
        f"PERSON_ROWS={person_count} DETAIL_ROWS={detail_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()