from pyspark import SparkContext

# khởi tạo sparkcontext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# đọc file occupation.txt để lấy tên nghề nghiệp 
occ_names_raw = sc.textFile("occupation.txt").collect()
occ_name_map = {}
for line in occ_names_raw:
    parts = line.split(',')
    occ_name_map[parts[0]] = parts[1] # { "1": "programmer", ... }

# bước 1: tạo dictionary từ users.txt với mapping userid -> occupation
users_raw = sc.textFile("users.txt").collect()
user_to_occ = {}

for line in users_raw:
    parts = line.split(',')
    u_id = parts[0]
    o_id = parts[3] 
    user_to_occ[u_id] = occ_name_map.get(o_id, o_id)
occ_broadcast = sc.broadcast(user_to_occ)
# bước 2: với mỗi rating, gán thông tin occupation theo userid
ratings_raw = sc.textFile("ratings_1.txt,ratings_2.txt")
def map_rating_to_occupation(line: str):
    parts = line.split(',')
    u_id = parts[0]
    rating = float(parts[2])
    # lấy tên nghề nghiệp từ dictionary đã broadcast
    occupation = occ_broadcast.value.get(u_id, "unknown")
    # bước 3: tạo cặp key-value với key là occupation và value là (rating, 1)
    return (occupation, (rating, 1))
occ_ratings_mapped = ratings_raw.map(map_rating_to_occupation)

# bước 4: reduce để tính tổng điểm và số lượt cho mỗi occupation, sau đó tính trung bình rating
def sum_ratings(v1, v2):
    # cộng dồn điểm và lượt đánh giá
    return (v1[0] + v2[0], v1[1] + v2[1])

# tính tổng
occ_totals = occ_ratings_mapped.reduceByKey(sum_ratings)
def calculate_avg(row):
    occ_name, (total_score, total_count) = row
    # tính trung bình: tổng điểm / tổng lượt
    return (occ_name, total_score / total_count, total_count)

final_results = occ_totals.map(calculate_avg)

# in kết quả ra màn hình
print("\n" + "="*60)
print(f"{'nghề nghiệp':<25} | {'điểm tb':<10} | {'lượt đánh giá'}")
print("-" * 60)
# sắp xếp theo tên nghề nghiệp 
sorted_data = final_results.sortBy(lambda x: x[0]).collect()
for name, avg, count in sorted_data:
    print(f"{name:<25} | {avg:<10.2f} | {int(count)}")
