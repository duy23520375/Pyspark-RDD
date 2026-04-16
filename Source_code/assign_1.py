from pyspark import SparkContext

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# bước 1: đọc file movies.txt và tạo một map (movieid -> title)
movies_raw = sc.textFile("movies.txt")
def keep_id_title(line: str):
    parts = line.split(',')
    return (parts[0], parts[1])
movies_rdd = movies_raw.map(keep_id_title)

# bước 2: đọc file ratings_1.txt và ratings_2.txt, map movieid -> (rating, 1) 
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")
def keep_id_rating(line: str):
    parts = line.split(',')
    return (parts[1], (float(parts[2]), 1))
ratings_mapped = ratings_raw.map(keep_id_rating)

# bước 3: reduce để tính tổng điểm và số lượt đánh giá 
def get_total_ratings(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])
reduced_ratings = ratings_mapped.reduceByKey(get_total_ratings)

# bước 4: tính điểm trung bình 
def calculate_avg(row):
    movie_id, (total_rating, total_count) = row
    return (movie_id, (total_rating / total_count, total_count))
avg_ratings_all = reduced_ratings.map(calculate_avg)

# bước 5: join để lấy tên phim 
final_result_all = avg_ratings_all.join(movies_rdd)

# In kết quả (tất cả phim)
print("\n" + "="*70)
print(f"{'id':<5} | {'tên phim':<30} | {'điểm tb':<10} | {'lượt đánh giá'}")
print("-" * 70)

all_data = final_result_all.collect()
for item in all_data:
    m_id, ((avg, count), title) = item
    print(f"{m_id:<5} | {title[:30]:<30} | {avg:<10.2f} | {int(count)}")

# bước 6: lọc và tìm phim cao nhất 
def filter_for_top(row):
    return row[1][0][1] >= 5
def get_score(row):
    return row[1][0][0]
print("-" * 70)
top_candidates = final_result_all.filter(filter_for_top)

if not top_candidates.isEmpty():
    top_movie = top_candidates.max(get_score)
    t_id, ((t_avg, t_count), t_title) = top_movie
    print(f"Phim điểm cao nhất (xét phim >= 5 lượt đánh giá):")
    print(f"Tên: {t_title}")
    print(f"Điểm TB: {t_avg:.2f}")
    print(f"Lượt đánh giá: {int(t_count)}")
