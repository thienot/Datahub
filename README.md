1. Docker Compose – Container Status

Ảnh chụp kết quả docker compose ps hoặc docker ps, thể hiện các container chính đang hoạt động.

Các service cần thể hiện:

Spark Master
Spark Worker
Airflow Webserver
Airflow Scheduler
PostgreSQL

Trạng thái mong muốn:

Spark Master       Up
Spark Worker       Up
Airflow Scheduler  Up
Airflow Webserver  Up / healthy
PostgreSQL         Up / healthy

Screenshot:
(/images/docker-run.png)

2. Spark Master UI

Truy cập:

http://localhost:8080

Ảnh cần thể hiện:

Spark Master đang hoạt động.
Worker đã đăng ký thành công với Master.
Worker ở trạng thái ALIVE.
Số Core và Memory của Worker.

Screenshot:
(/images/spark-master-ui.png)

(/images/spark-worker.png)