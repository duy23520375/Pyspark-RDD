from pyspark import SparkContext

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# bước 1: tạo map (userid -> gender)
users_raw = sc.textFile("users.txt")
def map_user_gender(line: str):
    parts = line.split(',')
    return (parts[0], parts[1]) # (userid, gender)
users_gender_rdd = users_raw.map(map_user_gender)

# bước 2: join với ratings để thêm thông tin giới tính 
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")
def map_rating_user(line: str):
    parts = line.split(',')
    return (parts[0], (parts[1], float(parts[2]))) # (userid, (movieid, rating))
ratings_user_rdd = ratings_raw.map(map_rating_user)

# join theo userid
joined_user_rating = users_gender_rdd.join(ratings_user_rdd)

# chuẩn bị key: ((movieid, gender), (rating, 1))
def prepare_for_avg(row):
    gender = row[1][0]
    movie_id = row[1][1][0]
    rating = row[1][1][1]
    return ((movie_id, gender), (rating, 1))
movie_gender_mapped = joined_user_rating.map(prepare_for_avg)

# bước 3: tính trung bình rating cho mỗi phim theo từng giới tính
def sum_ratings(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])
movie_gender_totals = movie_gender_mapped.reduceByKey(sum_ratings)

def calculate_avg(row):
    (movie_id, gender), (total_score, count) = row
    return (movie_id, (gender, total_score / count, count))
movie_gender_avg = movie_gender_totals.map(calculate_avg)

# lấy tên phim để in kết quả
movies_raw = sc.textFile("movies.txt")
movies_rdd = movies_raw.map(lambda l: (l.split(',')[0], l.split(',')[1]))
final_result = movie_gender_avg.join(movies_rdd)

# In kết quả với tiêu đề giới tính 
print("\n" + "="*80)
print(f"{'id':<5} | {'tên phim':<30} | {'giới tính':<10} | {'điểm tb':<10} | {'lượt'}")
print("-" * 80)
# sắp xếp theo id phim
results = final_result.sortBy(lambda x: x[0]).collect()

for m_id, ((gender, avg, count), title) in results:
    gender_label = "nam" if gender == 'M' else "nữ"
    print(f"{m_id:<5} | {title[:30]:<30} | {gender_label:<10} | {avg:<10.2f} | {int(count)}")

