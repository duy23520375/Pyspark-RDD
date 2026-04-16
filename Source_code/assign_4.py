from pyspark import SparkContext

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# hàm phân loại nhóm tuổi 
def get_age_group(age):
    age = int(age)
    if age <= 35:
        return "18-35"
    elif age <= 50:
        return "36-50"
    else:
        return "trên 50"

# bước 1: tạo map (userid -> age group) 
users_raw = sc.textFile("users.txt")
def map_user_age(line: str):
    parts = line.split(',')
    user_id = parts[0]
    age_group = get_age_group(parts[2]) 
    return (user_id, age_group)
users_age_rdd = users_raw.map(map_user_age)

# bước 2: join với ratings để thêm nhóm tuổi 
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")
def map_rating_user(line: str):
    parts = line.split(',')
    # (userid, (movieid, rating))
    return (parts[0], (parts[1], float(parts[2]))) 
ratings_user_rdd = ratings_raw.map(map_rating_user)

# join theo userid: (userid, (age_group, (movieid, rating)))
joined_age_rating = users_age_rdd.join(ratings_user_rdd)

def prepare_for_avg(row):
    age_group = row[1][0]
    movie_id = row[1][1][0]
    rating = row[1][1][1]
    return ((movie_id, age_group), (rating, 1))
movie_age_mapped = joined_age_rating.map(prepare_for_avg)


# bước 3: tính trung bình điểm đánh giá theo nhóm tuổi 
def sum_ratings(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

# cộng dồn điểm và lượt đánh giá theo cặp (movieid, age_group)
movie_age_totals = movie_age_mapped.reduceByKey(sum_ratings)

def calculate_avg(row):
    (movie_id, age_group), (total_score, count) = row
    return (movie_id, (age_group, total_score / count, count))
movie_age_avg = movie_age_totals.map(calculate_avg)

# lấy thêm tên phim từ movies.txt để hiển thị
movies_raw = sc.textFile("movies.txt")
movies_rdd = movies_raw.map(lambda l: (l.split(',')[0], l.split(',')[1]))
final_result = movie_age_avg.join(movies_rdd)

# In kết quả 
print("\n" + "="*85)
print(f"{'id':<5} | {'tên phim':<30} | {'nhóm tuổi':<12} | {'điểm tb':<10} | {'lượt'}")
print("-" * 85)
results = final_result.sortBy(lambda x: x[0]).collect()
for m_id, ((age_group, avg, count), title) in results:
    print(f"{m_id:<5} | {title[:30]:<30} | {age_group:<12} | {avg:<10.2f} | {int(count)}")

