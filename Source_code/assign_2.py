from pyspark import SparkContext

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# bước 1: tạo map (movieid -> list of genres) 
movies_raw = sc.textFile("movies.txt")

def map_movie_to_genres(line: str):
    parts = line.split(',')
    movie_id = parts[0]
    # tách các thể loại bằng dấu gạch đứng |
    genres = parts[2].split('|')
    return (movie_id, genres)
movie_genres_rdd = movies_raw.map(map_movie_to_genres)

# bước 2: map từ movieid -> rating -> (genre, rating)
# đọc file ratings
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")
def map_rating_data(line: str):
    parts = line.split(',')
    return (parts[1], float(parts[2])) # (movieid, rating)
ratings_rdd = ratings_raw.map(map_rating_data)

# join hai rdd lại để kết hợp thể loại và điểm số
joined_rdd = movie_genres_rdd.join(ratings_rdd)

def expand_genres(row):
    genres_list = row[1][0]
    rating = row[1][1]
    # tạo ra danh sách các cặp (genre, (rating, 1))
    return [(genre, (rating, 1)) for genre in genres_list]

# dùng flatmap để "trải phẳng" danh sách: mỗi thể loại thành 1 dòng riêng
genre_ratings_mapped = joined_rdd.flatMap(expand_genres)

# bước 3: tính trung bình điểm đánh giá cho từng thể loại 
def sum_ratings(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

# cộng dồn theo key là tên thể loại
genre_totals = genre_ratings_mapped.reduceByKey(sum_ratings)
def calculate_genre_avg(row):
    genre, (total_score, count) = row
    return (genre, total_score / count, count)
final_genre_stats = genre_totals.map(calculate_genre_avg)

# In kết quả 
print("\n" + "="*50)
print(f"{'thể loại':<20} | {'điểm tb':<10} | {'lượt đánh giá'}")
print("-" * 50)

# sắp xếp theo điểm trung bình giảm dần
results = final_genre_stats.sortBy(lambda x: x[1], ascending=False).collect()
for genre, avg, count in results:
    print(f"{genre:<20} | {avg:<10.2f} | {int(count)}")

