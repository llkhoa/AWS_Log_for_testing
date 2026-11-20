# Hướng dẫn làm việc với dự án AWS Pipeline Logs

## Mục tiêu

Thu thập thông tin và log của pipeline S3 → AWS Glue → Redshift bằng Python/boto3, lưu snapshot JSON trên máy. Giao tiếp và thông báo CLI bằng tiếng Việt, dùng UTF-8.

## Tài liệu cần đọc

- Đọc `RULES.MD` và `AWS_INSTRUCTIONS.md` trước khi làm việc liên quan AWS.
- `AWS_INSTRUCTIONS.md` mô tả tài nguyên và kiến trúc dự kiến. Đối chiếu cấu hình/code và kết quả AWS trước khi coi mô tả đó là trạng thái đang hoạt động.
- Không suy ra tên Glue Job từ tên file PySpark. Các đường dẫn pipeline trong tài liệu có thể thuộc dự án khác; kiểm tra tồn tại trước khi sử dụng.

## Cấu trúc code

| File | Chức năng |
| --- | --- |
| `log_s3.py` | Xuất metadata object theo bucket/prefix, số lượng và dung lượng được liệt kê. Đây không phải lịch sử truy cập S3. |
| `log_cloudwatch.py` | Xuất sự kiện CloudWatch theo log group, stream prefix và khoảng thời gian. |
| `log_glue.py` | Xuất trạng thái, thời gian và lỗi các Glue Job Run. |
| `log_redshift_data.py` | Dùng Redshift Data API đọc `sys_query_history`, `sys_load_history`, `sys_load_error_detail`. |
| `aws_config.py` | Nạp `.env`, tạo session/client, xử lý CLI và lưu JSON dùng chung. |
| `aws_s3_download.py` | Script tương tác liệt kê object; có `--dry-run`, hiện không tải nội dung file. |
| `pipeline_logs.py` | Lấy snapshot một Glue Run và log CloudWatch liên quan, lưu `run.json` và `events.jsonl`. |

Giữ các script cũ hoạt động khi sửa bộ 4 script, trừ khi người dùng yêu cầu thay thế.

## Cấu hình

`.env` nằm cạnh `aws_config.py`. Thứ tự ưu tiên cấu hình CLI: tham số dòng lệnh → biến môi trường hiện có → `.env` → giá trị mặc định trong code. Giá trị rỗng trong `.env` được bỏ qua.

- AWS: `AWS_DEFAULT_REGION`, `AWS_PROFILE`; hoặc `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` và `AWS_SESSION_TOKEN` nếu dùng credentials tạm thời.
- S3: `S3_BUCKET`, `S3_PREFIX`.
- Glue: `GLUE_JOB_NAME`, `GLUE_RUN_ID`.
- CloudWatch: `CLOUDWATCH_LOG_GROUPS` (phân cách bằng dấu phẩy), `CLOUDWATCH_LOG_STREAM_PREFIX`.
- Redshift: `REDSHIFT_DATABASE` và đúng một trong `REDSHIFT_CLUSTER_IDENTIFIER`, `REDSHIFT_WORKGROUP_NAME`; tùy phương thức xác thực có `REDSHIFT_DB_USER` hoặc `REDSHIFT_SECRET_ARN`.
- Đầu ra: `PIPELINE_LOG_OUTPUT`, mặc định `pipeline_logs`; đường dẫn tương đối tính từ thư mục chạy lệnh.

Không tự ghi đè `.env` của người dùng. Không in/copy access key, secret key hoặc session token vào terminal, tài liệu, log hay commit. Khi chẩn đoán, chỉ báo biến có/không được cấu hình hoặc vấn đề định dạng. Duy trì `.gitignore` cho `.env` và thư mục log mặc định.

## Cách chạy

Chạy trong thư mục dự án bằng PowerShell:

```powershell
python -m pip install -r requirements.txt
python log_s3.py
python log_cloudwatch.py --hours 24
python log_glue.py
python log_redshift_data.py --view load
python log_redshift_data.py --view query
python log_redshift_data.py --view load-errors
```

Mỗi script hỗ trợ `--help`, `--region`, `--profile`, `--output`. Bộ 4 script lưu file JSON riêng có tên dịch vụ, thời điểm UTC và mã ngẫu nhiên để tránh ghi đè. Các tùy chọn `--limit` giới hạn kết quả và đầu ra có cờ `truncated`.

## Quy tắc triển khai

- Đọc skill `aws-sdk-python-usage` khi sửa code boto3, cùng tài liệu S3 đi kèm nếu thao tác S3.
- Dùng session/client dùng lại và đóng client; đặt timeout và retry hữu hạn.
- Dùng paginator cho API hỗ trợ phân trang; không bỏ sót trang hoặc che giấu việc cắt kết quả.
- Giữ lỗi rõ ràng, trả exit code khác 0 khi thất bại. Không chuyển `AccessDenied`, lỗi xác thực hoặc query thất bại thành kết quả thành công/rỗng.
- Chức năng hiện tại phục vụ đọc và xuất dữ liệu. Không tự tạo/xóa bucket, khởi chạy Glue Job, thay IAM, hoặc chạy SQL thay đổi dữ liệu khi chỉ được yêu cầu lấy log.
- Redshift chỉ chạy các truy vấn SELECT đã định nghĩa. Nếu hết thời gian chờ, báo statement ID và khả năng query vẫn đang chạy.
- Không lưu toàn bộ Glue Job Arguments vì có thể chứa thông tin nhạy cảm.

## Kiểm tra và báo cáo

- Kiểm tra `--help` của script thay đổi và chạy kiểm thử phù hợp bằng `botocore.stub.Stubber` hoặc mock.
- Với thay đổi thu thập dữ liệu, kiểm tra nhiều trang, dữ liệu rỗng, giới hạn kết quả, lỗi AWS và nội dung JSON được lưu.
- `python aws_s3_download.py --dry-run` chỉ kiểm tra script S3 cũ; không chứng minh cả 4 script đã chạy thật.
- Khi người dùng yêu cầu chạy thật, dùng cấu hình hiện có, báo riêng kết quả từng dịch vụ và đường dẫn file. Nếu thiếu cấu hình, nêu đúng tên biến cần điền.
- Phân biệt kiểm thử mô phỏng với truy vấn AWS thật. Không ghi trạng thái kiểm tra cũ thành bảo đảm hiện tại.
- Snapshot CloudWatch có thể thiếu sự kiện đến trễ. Không có stream prefix nghĩa là có thể đọc log nhiều lần chạy trong cùng group.
- Lịch sử Redshift hiện lọc theo database/thời gian, chưa tự liên kết với một Glue Run. Quyền database ảnh hưởng phạm vi lịch sử nhìn thấy.
- Glue thành công, query thành công hoặc file log được tạo không đủ để xác nhận toàn pipeline hoàn tất hay giao dịch Redshift đã commit.
