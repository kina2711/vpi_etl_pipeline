# VPI Data Pipeline (PostgreSQL to Google Sheets)

[Tiếng Việt](#tiếng-việt) | [English](#english)

---

# Tiếng Việt

## 1. Giới thiệu
Dự án này là một pipeline ETL (Extract, Transform, Load) tự động, được thiết kế để trích xuất dữ liệu báo cáo từ cơ sở dữ liệu PostgreSQL (eHC) của VPI, xử lý và tải lên Google Sheets.

Hệ thống được xây dựng để hoạt động bền bỉ, tự động xử lý các giới hạn của Google API và lỗi mạng, đồng thời hỗ trợ xử lý lượng dữ liệu lớn thông qua cơ chế chia lô (batching).

## 2. Tính năng Chính
* **Cấu hình linh hoạt**: Quản lý báo cáo qua `config.ini` mà không cần sửa code.
* **Batch Processing (Chia lô)**:
    * **Date Batching**: Chia khoảng thời gian báo cáo thành các lô nhỏ (ví dụ: 5 ngày) để giảm tải cho Database.
    * **Row Batching**: Chia dữ liệu ghi lên Sheets thành các lô nhỏ (ví dụ: 1000 dòng) để tránh timeout.
* **Cơ chế Retry & Ổn định**: Tự động thử lại khi gặp lỗi mạng (`WinError 10053`, `ConnectionAborted`) hoặc lỗi Quota (`429`).
* **Chiến lược tải dữ liệu (Load Strategy)**: Hỗ trợ `append` (ghi nối tiếp) và `overwrite` (ghi đè).
* **Xử lý xóa thông minh**: Hỗ trợ xóa nội dung (`clear_content`) hoặc xóa hàng (`delete_rows` - có thuật toán lách luật giới hạn của Google Sheets).
* **Bảo mật**: Tách biệt mã nguồn và thông tin nhạy cảm (mật khẩu DB) bằng `.env.local`.

## 3. Cấu trúc Dự án

```

etl\_vpi\_project/
│
├── app/                   \# Mã nguồn chính
│   ├── config/            \# Đọc cấu hình (settings.py)
│   ├── connectors/        \# Kết nối Postgres & Sheets (có retry logic)
│   ├── pipelines/         \# Logic ETL (ReportPipeline)
│   ├── utils/             \# Tiện ích (xử lý ngày, chunk data)
│   └── **main**.py        \# Điểm khởi chạy (CLI)
│
├── sql/                   \# Các file truy vấn SQL
│   ├── vpi\_khachhang.sql
│   ├── vpi\_doanhthu.sql
│   └── vpi\_duyetthanhtoan.sql
│
├── config.ini             \# File cấu hình pipeline (Logic chạy)
├── .env.local             \# File chứa thông tin mật (DB Creds, API paths)
├── .env.sample            \# File mẫu cho .env.local
├── requirements.txt       \# Các thư viện cần thiết
└── pipeline.log           \# File log hoạt động

````

## 4. Cài đặt

1.  **Clone dự án và tạo môi trường ảo:**
    ```bash
    python -m venv .venv
    # Windows
    .\.venv\Scripts\activate
    # macOS/Linux
    source .venv/bin/activate
    ```

2.  **Cài đặt thư viện:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Thiết lập biến môi trường:**
    * Copy file `.env.sample` thành `.env.local`.
    * Mở `.env.local` và điền thông tin kết nối Database thật.

4.  **Thiết lập Google API:**
    * Đặt file `client_secret.json` (tải từ Google Cloud Console) vào thư mục gốc.

5.  **Cấu hình Báo cáo:**
    * Mở `config.ini` để chỉnh sửa ID của Google Sheets và các tham số chạy.

## 5. Cấu hình (`config.ini`)

Giải thích các tham số quan trọng trong `config.ini`:

* `date_range_strategy`:
    * `today_only`: Chỉ lấy dữ liệu hôm nay.
    * `month_to_date`: Từ đầu tháng đến hôm nay.
    * `previous_month`: Toàn bộ tháng trước.
* `load_strategy`:
    * `append`: Ghi nối tiếp vào cuối sheet (Không xóa dữ liệu cũ).
    * `overwrite`: Xóa dữ liệu cũ trước khi ghi mới.
* `clear_method` (Chỉ dùng khi `overwrite`):
    * `clear_content`: Chỉ xóa nội dung text, giữ nguyên định dạng hàng (Cần `clear_end_column`).
    * `delete_rows`: Xóa vật lý các hàng (Giúp giảm dung lượng file, tránh lỗi 10 triệu ô).

## 6. Sử dụng

Đảm bảo môi trường ảo đã được kích hoạt.

* **Chạy tất cả báo cáo (Theo thứ tự mặc định):**
    ```bash
    python -m app
    ```

* **Chạy báo cáo cụ thể:**
    ```bash
    python -m app --report BRANCH_VPI_DOANHTHU
    ```

* **Chạy nhiều báo cáo:**
    ```bash
    python -m app --report BRANCH_VPI_KHACHHANG --report BRANCH_VPI_DUYETTHANHTOAN
    ```

---

# English

## 1. Introduction
This project is an automated ETL (Extract, Transform, Load) pipeline designed to extract report data from the VPI PostgreSQL database (eHC), process it, and load it into Google Sheets.

The system is built for robustness, automatically handling Google API limitations and network errors, while supporting large datasets via batch processing.

## 2. Key Features
* **Configuration Driven**: Manage pipelines via `config.ini` without code changes.
* **Batch Processing**:
    * **Date Batching**: Splits report date ranges into smaller chunks (e.g., 5 days) to reduce Database load.
    * **Row Batching**: Splits data upload into smaller chunks (e.g., 1000 rows) to avoid API timeouts.
* **Retry Mechanism**: Automatically retries on network errors (`WinError 10053`, `ConnectionAborted`) or API Quota errors (`429`).
* **Load Strategies**: Supports `append` and `overwrite`.
* **Smart Clearing**: Supports clearing content (`clear_content`) or deleting rows physically (`delete_rows` - implements a workaround for Google's "delete all non-frozen rows" limitation).
* **Security**: Separates code and credentials using `.env.local`.

## 3. Project Structure

````

etl\_vpi\_project/
│
├── app/                   \# Main source code
│   ├── config/            \# Config loader (settings.py)
│   ├── connectors/        \# Postgres & Sheets connectors (with retry logic)
│   ├── pipelines/         \# ETL Logic (ReportPipeline)
│   ├── utils/             \# Utilities (dates, helpers)
│   └── **main**.py        \# Entry point (CLI)
│
├── sql/                   \# SQL query files
│   ├── vpi\_khachhang.sql
│   ├── vpi\_doanhthu.sql
│   └── vpi\_duyetthanhtoan.sql
│
├── config.ini             \# Pipeline configuration
├── .env.local             \# Secrets (DB Creds, API paths)
├── .env.sample            \# Sample file for .env.local
├── requirements.txt       \# Python dependencies
└── pipeline.log           \# Activity log

````

## 4. Installation

1.  **Clone and Create Virtual Environment:**
    ```bash
    python -m venv .venv
    # Windows
    .\.venv\Scripts\activate
    # macOS/Linux
    source .venv/bin/activate
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Setup Environment Variables:**
    * Copy `.env.sample` to `.env.local`.
    * Fill in your actual Database credentials in `.env.local`.

4.  **Setup Google API:**
    * Place your `client_secret.json` (from Google Cloud Console) in the root directory.

5.  **Configure Reports:**
    * Edit `config.ini` to set your Google Spreadsheet IDs and running parameters.

## 5. Configuration (`config.ini`)

Key parameters explanation:

* `date_range_strategy`:
    * `today_only`: Data for today only.
    * `month_to_date`: From the 1st of the month to today.
    * `previous_month`: Entire previous month.
* `load_strategy`:
    * `append`: Append data to the end of the sheet (Preserves old data).
    * `overwrite`: Clear old data before writing new data.
* `clear_method` (Used with `overwrite`):
    * `clear_content`: Clears text content only (Requires `clear_end_column`).
    * `delete_rows`: Physically deletes rows (Helps reduce file size and avoid the 10M cell limit).

## 6. Usage

Ensure the virtual environment is activated.

* **Run All Reports (Default Order):**
    ```bash
    python -m app
    ```

* **Run Specific Report:**
    ```bash
    python -m app --report BRANCH_VPI_DOANHTHU
    ```

* **Run Multiple Reports:**
    ```bash
    python -m app --report BRANCH_VPI_KHACHHANG --report BRANCH_VPI_DUYETTHANHTOAN
    ```
