import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema_bhxh = StructType([
    StructField("ID", LongType(), nullable=False),
    StructField("NLD_ID", LongType(), nullable=False),

    StructField("SO_SO_BHXH", StringType(), nullable=True),
    StructField("THANG_BD", StringType(), nullable=True),
    StructField("THANG_KT", StringType(), nullable=True),

    StructField("TT_TG_BHXH", StringType(), nullable=True),
    StructField("DT_TG_BHXH", StringType(), nullable=True),
    StructField("NAM_TG_BHXH", IntegerType(), nullable=True),
    StructField("THANG_TG_BHXH", IntegerType(), nullable=True),

    StructField("NAM_TG_BHXH_BB", IntegerType(), nullable=True),
    StructField("THANG_TG_BHXH_BB", IntegerType(), nullable=True),

    StructField("TT_TG_BHTN", StringType(), nullable=True),
    StructField("DT_TG_BHTN", StringType(), nullable=True),
    StructField("NAM_TG_BHTN", IntegerType(), nullable=True),
    StructField("THANG_TG_BHTN", IntegerType(), nullable=True),

    StructField("TT_TG_BHYT", StringType(), nullable=True),
    StructField("DT_TG_BHYT", StringType(), nullable=True),
    StructField("NAM_TG_BHYT", IntegerType(), nullable=True),
    StructField("THANG_TG_BHYT", IntegerType(), nullable=True),

    StructField("NAM_NO_BHXH", IntegerType(), nullable=True),
    StructField("THANG_NO_BHXH", IntegerType(), nullable=True),

    StructField("NAM_NO_BHTN", IntegerType(), nullable=True),
    StructField("THANG_NO_BHTN", IntegerType(), nullable=True),

    StructField("TT_TG_BH", StringType(), nullable=True),
    StructField("DT_TG_BH", StringType(), nullable=True),

    StructField("TU_THANG_DVI", StringType(), nullable=True),
    StructField("DEN_THANG_DVI", StringType(), nullable=True),
    StructField("DEN_THANG_HTTT", StringType(), nullable=True),
    StructField("DEN_THANG_BHTN", StringType(), nullable=True),

    StructField("THANG_BD_LT", StringType(), nullable=True),
    StructField("THANG_KT_LT", StringType(), nullable=True),
    StructField("SO_THANG_LT", IntegerType(), nullable=True),

    StructField("IS_ERRORS", IntegerType(), nullable=True),
    StructField("NGHI_VIEC", IntegerType(), nullable=True),
    StructField("IS_CONTINUE", IntegerType(), nullable=True),
    StructField("TRUY_DONG", IntegerType(), nullable=True),

    StructField("DEN_NGAY", StringType(), nullable=True),

    StructField("MA_CD", StringType(), nullable=True),
    StructField("MA_NHH", StringType(), nullable=True),
    StructField("DD_MA_DON_VI", StringType(), nullable=True),

    StructField("DD_THANG_DONG_DEN_XH", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHXH", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_YT", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHYT", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_TN", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_BHTN", DecimalType(10, 4), nullable=True),

    StructField("DD_THANG_DONG_DEN_TNLD", StringType(), nullable=True),
    StructField("DD_TY_LE_NO_TNLD", DecimalType(10, 4), nullable=True),

    StructField("RAW_RESPONSE", StringType(), nullable=True),

    StructField(
        "CREATED_AT",
        TimestampType(),
        nullable=False,
    ),
])

schema__bhxh_detail = StructType([
    StructField("ID", LongType(), nullable=False),
    StructField("MASTER_ID", LongType(), nullable=False),
    StructField("NLD_ID", LongType(), nullable=False),

    StructField("DOT_PHAT_SINH", StringType(), nullable=True),
    StructField("TU_THANG", StringType(), nullable=True),
    StructField("DEN_THANG", StringType(), nullable=True),
    StructField("MA_DON_VI", StringType(), nullable=True),
    StructField("TEN_DON_VI", StringType(), nullable=True),

    StructField("LOAI_DT", StringType(), nullable=True),
    StructField("LOAI", IntegerType(), nullable=True),
    StructField("PA", StringType(), nullable=True),
    StructField("DON_VI_TINH", StringType(), nullable=True),
    StructField("MA_NT", StringType(), nullable=True),

    StructField("CHUC_DANH_CV", StringType(), nullable=True),
    StructField("CHUC_DANH_CV_PRE", StringType(), nullable=True),
    StructField("NOI_LAM_VIEC", StringType(), nullable=True),
    StructField("NOI_DUNG", StringType(), nullable=True),

    StructField("MUC_LUONG", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_TN", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_BHYT", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_PC", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_BS", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_NLD", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_NSNN", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_HS", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_TT", DecimalType(19, 4), nullable=True),

    StructField("HS_LUONG", DecimalType(10, 4), nullable=True),
    StructField("PC_CHUC_VU", DecimalType(10, 4), nullable=True),
    StructField("PC_THAM_NIEN", DecimalType(10, 4), nullable=True),
    StructField("PC_NGHE", DecimalType(10, 4), nullable=True),
    StructField("PC_KHU_VUC", DecimalType(10, 4), nullable=True),
    StructField("PC_KHAC", DecimalType(10, 4), nullable=True),
    StructField("PC_TAI_CU", DecimalType(10, 4), nullable=True),
    StructField("HS_TN", DecimalType(10, 4), nullable=True),
    StructField("HS_NG", DecimalType(10, 4), nullable=True),
    StructField("HS_TC", DecimalType(10, 4), nullable=True),

    StructField("TYLE_BHXH", DecimalType(10, 4), nullable=True),
    StructField("TYLE_BHYT", DecimalType(10, 4), nullable=True),
    StructField("TYLE_BHTN", DecimalType(10, 4), nullable=True),
    StructField("TYLE_TUDV", DecimalType(10, 4), nullable=True),
    StructField("TYLE_HTTT", DecimalType(10, 4), nullable=True),
    StructField("TYLE_ODTS", DecimalType(10, 4), nullable=True),
    StructField("TYLE_TNLD", DecimalType(10, 4), nullable=True),
    StructField("TYLE_NSNN", DecimalType(10, 4), nullable=True),

    StructField("DK1", IntegerType(), nullable=True),
    StructField("DK2", IntegerType(), nullable=True),
    StructField("DK3", IntegerType(), nullable=True),
    StructField("DK4", IntegerType(), nullable=True),
    StructField("DK5", IntegerType(), nullable=True),
    StructField("DK6", IntegerType(), nullable=True),

    StructField("IS_BHXH", IntegerType(), nullable=True),
    StructField("IS_BHXH_BB", IntegerType(), nullable=True),
    StructField("IS_BHTN", IntegerType(), nullable=True),
    StructField("IS_BHYT", IntegerType(), nullable=True),
    StructField("IS_BHXH2", IntegerType(), nullable=True),
    StructField("IS_BHTN2", IntegerType(), nullable=True),

    StructField("IS_ERROR", IntegerType(), nullable=True),
    StructField("IS_TR", IntegerType(), nullable=True),
    StructField("IS_BONUS", IntegerType(), nullable=True),
    StructField("ML_TC", IntegerType(), nullable=True),

    StructField("GHI_CHU", StringType(), nullable=True),
    StructField("KIEM_TRA", IntegerType(), nullable=True),
    StructField("SO_THANG", IntegerType(), nullable=True),
    StructField("MA_KHOI_TK", StringType(), nullable=True),

    StructField("TY_LE_DONG", DecimalType(10, 4), nullable=True),
    StructField("MUC_DONG", DecimalType(19, 4), nullable=True),

    StructField("LUONG_CHINH", DecimalType(19, 4), nullable=True),

    StructField("CHUC_DANH_NLV", StringType(), nullable=True),
    StructField("PHUONG_THUC", StringType(), nullable=True),
    StructField("PHUONG_THUC_DONG", StringType(), nullable=True),

    StructField("MUC_LUONG_PRE", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_PC_PRE", DecimalType(19, 4), nullable=True),
    StructField("MUC_LUONG_BS_PRE", DecimalType(19, 4), nullable=True),
    StructField("HS_LUONG_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_CHUC_VU_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_THAM_NIEN_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_NGHE_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_KHU_VUC_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_KHAC_PRE", DecimalType(10, 4), nullable=True),
    StructField("PC_TAI_CU_PRE", DecimalType(10, 4), nullable=True),
    StructField("LUONG_CHINH_PRE", DecimalType(19, 4), nullable=True),

    # Audit
    StructField("CREATED_AT", TimestampType(), nullable=False),
])


EXPECTED_MASTER_ROWS = 142857
EXPECTED_DETAIL_ROWS = 1000000


def main():
    #Dùng thư viện argprase của Python để đọc tham số dòng lệnh
    #Cách Script Python nhận input khi gọi spark-submit
    parser = argparse.ArgumentParser()

    #Khai báo tham số bắt buộc truyền vào 
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bronze_qttg").getOrCreate()

    #Đường dẫn file raw master bhxh
    master_path = f"{args.input_dir}/RAW_QTTG_BHXH.csv"

    #Đường dẫn file raw detail bhxh
    detail_path = f"{args.input_dir}/RAW_QTTG_BHXH_DETAIL.csv"

    #Đọc file raw master bhxh
    master_df = (
        spark.read.option("header", "true")
        .schema(schema_bhxh)
        .csv(master_path)
    )

     #Đọc file raw detail bhxh
    detail_df = (
        spark.read.option("header", "true")
        .schema(schema__bhxh_detail)
        .csv(detail_path)
    )

    #Đếm số dòng của master bhxh, detail
    master_count = master_df.count()
    detail_count = detail_df.count()

    #Kiểm tra so sánh số hàng của master khi đọc dữ liệu vào, nếu kh khớp in ra kết quả lỗi
    if master_count != EXPECTED_MASTER_ROWS:
        print(
            f"LAYER=BRONZE STATUS=FAILED "
            f"REASON=master_rows_mismatch EXPECTED={EXPECTED_MASTER_ROWS} ACTUAL={master_count}"
        )
        spark.stop()
        sys.exit(1)

    #Cũng kiểm tra so sánh số hàng của detail khi đọc dữ liệu vào, nếu kh khớp in ra kết quả lỗi
    if detail_count != EXPECTED_DETAIL_ROWS:
        print(
            f"LAYER=BRONZE STATUS=FAILED "
            f"REASON=detail_rows_mismatch EXPECTED={EXPECTED_DETAIL_ROWS} ACTUAL={detail_count}"
        )
        spark.stop()
        sys.exit(1)

    #Sau đó lưu file lại
    master_df.write.mode("overwrite").parquet(f"{args.output_dir}/master")
    detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/detail")

    print(
        f"LAYER=BRONZE STATUS=SUCCESS "
        f"MASTER_ROWS={master_count} DETAIL_ROWS={detail_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()