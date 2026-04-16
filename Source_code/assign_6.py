from pyspark import SparkContext
from datetime import datetime

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# sử dụng hàm trợ giúp để chuyển đổi timestamp (dạng unix) thành năm (year)
def get_year_from_timestamp(ts_str):
    try:
        # chuyển chuỗi sang số nguyên
        ts = int(ts_str)
        # chuyển unix timestamp sang đối tượng datetime và lấy năm
        return str(datetime.fromtimestamp(ts).year)
    except:
        return "unknown"
# bước 1: đọc dữ liệu ratings (từ cả ratings_1.txt và ratings_2.txt)
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")

# bước 2 & 3: chuyển đổi timestamp thành năm và phát hành cặp key-value
def map_rating_to_year(line: str):
    parts = line.split(',')
    # định dạng file: userid, movieid, rating, timestamp
    rating = float(parts[2])
    timestamp = parts[3]
    year = get_year_from_timestamp(timestamp)
    # phát hành cặp key-value với key là năm và value là (rating, 1)
    return (year, (rating, 1))
year_ratings_mapped = ratings_raw.map(map_rating_to_year)
# bước 4: reduce để tính tổng điểm và số lượt cho mỗi năm, sau đó tính trung bình
def sum_ratings(v1, v2):
    # cộng dồn điểm và lượt đánh giá
    return (v1[0] + v2[0], v1[1] + v2[1])
# tính tổng theo năm
year_totals = year_ratings_mapped.reduceByKey(sum_ratings)
def calculate_avg(row):
    year, (total_score, total_count) = row
    # tính trung bình: tổng điểm / tổng lượt
    return (year, total_score / total_count, total_count)
final_year_stats = year_totals.map(calculate_avg)

# in kết quả ra màn hình
print("\n" + "="*50)
print(f"{'năm':<10} | {'điểm tb':<15} | {'tổng lượt đánh giá'}")
print("-" * 50)
# sắp xếp theo năm tăng dần 
sorted_results = final_year_stats.sortBy(lambda x: x[0]).collect()
for year, avg, count in sorted_results:
    print(f"{year:<10} | {avg:<15.2f} | {int(count)}")
