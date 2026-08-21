# XÂY DỰNG DATA PIPELINE & RESTFUL API TRA CỨU THÔNG TIN

## Mục tiêu bài tập
*   Hiểu và thực hành thiết kế Cơ sở dữ liệu quan hệ (Mô hình 1-N / Header - Detail).
*   Làm quen với framework Java Spring Boot để phát triển RESTful API.
*   Áp dụng các tiêu chuẩn thực tế khi làm API: Phân trang, Xử lý lỗi (Exception Handling), Viết tài liệu (Swagger), và Logging.

## Luồng kiến trúc (Architecture Flow)
`Dữ liệu (Silver Layer)  == (Spark Job) ==>  Relational DB (Postgres/Oracle)  ==> (Spring Boot) ==  Người dùng gọi API`
### 1. Spark Job & Database

#### 1.1 Spark Job chạy thành công

Spark Job được thực thi thành công và không phát sinh lỗi.

![Spark Job Success](images/01-spark-job-success.png)

#### 1.2 Database có dữ liệu

Kiểm tra dữ liệu sau khi Spark Job hoàn thành:

Bảng Header
![Database Data](images/02-database-data.png)

Bảng Detail
![Database Data](images/03-database-data.png)

#### 1.3 Kiểm tra quan hệ giữa các bảng

Quan hệ các bảng
![Database Relationship](images/03-database-relationship.png)

---

### 2. API Search

#### 2.1 Search theo Tên
Api Search theo tên
![Api Search Name](images/01-api-name.png)

#### 2.1 Search theo Mã
Api Search theo mã
![Api Search Key](images/02-api-key.png)

---

### 3. Chỉnh size để dữ liệu cắt theo trang

Cắt size = 2
![ Size](images/01-size.png)

### 4. Truyền sai tham số

Truyền sai tham số page
![ Page](images/error-page.png)

Truyền sai tham số size
![ Size](images/error-size.png)

Truyền sai thiếu tham số keywowrd
![ Key](images/error-key.png)

### 5. Truy cập được Swagger UI và gọi thử

Truy cập được Swagger UI và gọi thử
![ Swagger](images/swagger.png)



# Báo cáo: Airflow điều phối Spark — Bài tập QTTG BHXH (Local)

## 1. Mục tiêu

Xây dựng pipeline xử lý dữ liệu QTTG BHXH theo mô hình 4 lớp **Bronze → Silver → Gold → Validate**, dùng Spark để xử lý dữ liệu và Airflow để điều phối, chạy hoàn toàn trên môi trường local (Docker Desktop, Windows).

## 2. Kiến trúc hệ thống

| Thành phần          | Vai trò                   |                 Image                      |
| `spark-master`      | Điều phối tính toán Spark | `apache/spark:3.5.1-python3`               |
| `spark-worker`      | Thực thi tính toán Spark  | `apache/spark:3.5.1-python3`               |
| `postgres`          | Lưu metadata Airflow      | `postgres:15`                              |
| `airflow-webserver` | Giao diện quản lý DAG     | build từ `apache/airflow:2.8.1-python3.10` |
| `airflow-scheduler` | Điều phối chạy task       | build từ `apache/airflow:2.8.1-python3.10` |

Luồng xử lý:

Bronze (nạp CSV thô)
    ↓
Silver (chuẩn hoá, lấy bản ghi mới nhất theo SO_SO_BHXH)
    ↓
Gold (tổng hợp báo cáo theo tháng)
    ↓
Validate (kiểm tra tính toàn vẹn dữ liệu)

Airflow gọi Spark thông qua `docker exec` vào container `spark-master`, không xử lý dữ liệu trực tiếp — chỉ điều phối thứ tự và giám sát kết quả.


## 3. Bằng chứng thực hiện (ảnh chụp màn hình)

### 3.1. Hệ thống container đang chạy

![Docker Compose PS](images/01_docker_compose_ps.png)

Toàn bộ 5 container (`spark-master`, `spark-worker`, `postgres`, `airflow-webserver`, `airflow-scheduler`) ở trạng thái `running`/`healthy`.

### 3.2. Spark Master UI

![Spark Master UI](images/02_spark_master_ui.png)

Spark Master nhận diện đủ Worker ở trạng thái `ALIVE`.

### 3.3. Spark Worker UI

![Spark Worker UI](images/03_spark_worker_ui.png)

Worker kết nối đúng về `spark://spark-master:7077`.

### 3.4. Airflow — Danh sách DAG

![Airflow DAG list](images/04_airflow_dag_list.png)

DAG `qttg_bronze_silver_gold` đã được nạp thành công, không có lỗi import.

### 3.5. Kiểm tra kết nối Airflow → Docker → Spark

![Scheduler docker ps](images/05_scheduler_docker_ps.png)

Lệnh `docker compose exec airflow-scheduler docker ps` chạy thành công — xác nhận Airflow scheduler điều khiển được Docker Desktop.

### 3.6. Airflow Graph — luồng thực thi đầy đủ

![Airflow Graph](images/06_airflow_graph.png)

4 task nối liền theo đúng thứ tự phụ thuộc: `bronze_ingest_csv → silver_latest_person → gold_monthly_report → validate_results`, tất cả chuyển màu xanh lá (thành công).

### 3.7. Dữ liệu và code hiển thị đúng trên cả Master và Worker

![Data visible](images/07_data_visible_master_worker.png)

Cả `spark-master` và `spark-worker` đều thấy được 2 file CSV nguồn và các file PySpark.

### 3.8. Log Bronze

![Log Bronze](images/08_log_bronze.png)

```
LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS=142857 DETAIL_ROWS=1000000
```

Kết quả khớp đúng số dòng kỳ vọng của 2 file CSV nguồn.

### 3.9. Log Silver

![Log Silver](images/09_log_silver.png)

```
```

`PERSON_ROWS` nhỏ hơn hoặc bằng 142.857 — xác nhận đã lọc đúng, mỗi người chỉ còn 1 bản ghi mới nhất theo `SO_SO_BHXH`.

### 3.10. Log Gold

![Log Gold](images/10_log_gold.png)

```
SHOW 20 dòng kết quả
```

![Log Gold 2](images/11_log_gold.png)


### 4.11. Log Validate

![Log Validate](images/12_log_validate.png)

```
LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0
```

Không phát hiện lỗi toàn vẹn dữ liệu: không có detail mồ côi, không trùng người ở Silver, không trùng tháng ở Gold, khoảng thời gian hợp lệ.


