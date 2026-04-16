# Phân Tích Dữ Liệu với PySpark RDD

Bài tập này sử dụng Apache Spark (RDD API) để phân tích tập dữ liệu Movies, bao gồm thông tin về phim, người dùng và các lượt đánh giá (ratings).

## Cấu trúc thư mục

```text
RDD/
├── Source_code/           # Chứa các file Python xử lý RDD (Bài 1 - Bài 6)
│   ├── assign_1.py        # Tính điểm TB và tổng lượt đánh giá từng phim
│   ├── assign_2.py        # Phân tích đánh giá theo thể loại
│   ├── assign_3.py        # Phân tích đánh giá theo giới tính
│   ├── assign_4.py        # Phân tích đánh giá theo nhóm tuổi
│   ├── assign_5.py        # Phân tích đánh giá theo nghề nghiệp (Occupation)
│   └── assign_6.py        # Phân tích xu hướng đánh giá theo thời gian (Năm)
├── Results/               # Ảnh chụp kết quả chạy thực tế của từng bài
│   ├── assign_1.png
│   └── ...
├── movies.txt             
├── users.txt              
├── occupation.txt        
├── ratings_1.txt         
├── ratings_2.txt          
├── assignments.ipynb     
