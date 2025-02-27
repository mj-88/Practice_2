import mysql.connector
from collections import defaultdict, OrderedDict
import sys

class LFUCache:
    def __init__(self, capacity_bytes):
        self.capacity_bytes = capacity_bytes  # 총 캐시 크기 (bytes)
        self.current_size = capacity_bytes  # 현재 캐시에 저장된 데이터 크기 (bytes)
        self.cache = {}  # { (segnumber, videonumber, tileId): (value, frequency, fileSize) }
        self.freq_map = defaultdict(OrderedDict)  # {freq: { (segnumber, videonumber, tileId): None }}
        self.min_freq = 0  # 최소 사용 빈도 추적
        self.hit =0
        self.miss=0

    def _update_frequency(self, key):
        """기존 키의 사용 빈도를 업데이트"""
        value, freq, file_size = self.cache[key]

        # 이전 빈도에서 제거
        del self.freq_map[freq][key]
        if not self.freq_map[freq]:  # 해당 빈도가 비어있다면 삭제
            del self.freq_map[freq]

        # 새로운 빈도에 추가
        self.cache[key] = (value, freq + 1, file_size)
        self.freq_map[freq + 1][key] = None

    def get(self, segnumber, videonumber, tileId):
        """캐시에서 데이터 가져오기"""
        key = (segnumber, videonumber, tileId)
        if key not in self.cache:
            return None  # Cache Miss

        self._update_frequency(key)
        return self.cache[key][0]  # value 반환

    def put(self, segnumber, videonumber, tileId, value, fileSize):
        """캐시에 데이터 추가 (LFU 적용)"""
        key = (segnumber, videonumber, tileId)

        # 🔹 이미 캐시에 있는 데이터인지 확인 (LFU 정책 적용)
        existing_value = self.get(segnumber, videonumber, tileId)
        if existing_value is not None:
            self.hit +=1
            return  # 이미 존재하는 데이터이므로 빈도만 증가하고 종료
        
        self.miss+=1

        # 캐시 공간 확보: 현재 크기가 초과되면 LFU에 따라서서 삭제
        while self.current_size - fileSize < 0:
            if not self.cache:
                print('not self.cache')
                break  # 캐시에 데이터가 없으면 종료

            # 🔹 먼저 min_freq를 찾는다
            min_freq = min(self.freq_map.keys())  # 현재 존재하는 빈도 중 가장 작은 값 찾기

            # 🔹 빈도 리스트가 비어 있다면 삭제하고 다시 찾음
            while min_freq in self.freq_map and not self.freq_map[min_freq]:
                del self.freq_map[min_freq]
                if not self.freq_map:
                    print('freq_map is empty, skipping LFU removal.')
                    return  # freq_map이 완전히 비었다면 종료
                min_freq = min(self.freq_map.keys())  # 다시 최소 빈도 찾기

            # 🔹 가장 적게 사용된 항목 중 가장 오래된 항목 제거 (LFU 정책)
            lfu_key, _ = self.freq_map[min_freq].popitem(last=False)
            _, _, removed_size = self.cache[lfu_key]
                        
            del self.cache[lfu_key]
            self.current_size += removed_size  # 캐시 크기에서 제거된 파일 크기만큼 증가

            # 🔹 만약 해당 빈도가 모두 제거되었다면, freq_map에서 삭제
            if not self.freq_map[min_freq]:
                del self.freq_map[min_freq]


        # 새로운 키 추가
        self.cache[key] = (value, 1, fileSize)  # 초기 빈도 1
        self.freq_map[1][key] = None
        self.current_size -= fileSize  # 캐시 크기에서 파일 크기 차감
 

    def display_cache(self):
        """현재 캐시 상태 출력"""
        print(f"\n📌 현재 캐시 상태: {self.current_size} / {self.capacity_bytes} bytes")
        print(f"Cache Hit: {self.hit}, Cache Miss: {self.miss}")
        print("캐시에 저장된 데이터 목록:")
        for (segnumber, videonumber, tileId), (value, freq, fileSize) in self.cache.items():
            print(f"  ➡️ Segment={segnumber}, Video={videonumber}, TileID={tileId}, Size={fileSize} bytes, Frequency={freq}")
        print("-" * 50)

# ================= MySQL 데이터 로드 =================
def load_data_from_mysql():
    """MySQL에서 타일 데이터를 가져와 LFU 캐시에 저장"""
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="m2cl112358",
        database="tile_database"
    )
    
    cursor = conn.cursor()
    query = "SELECT segNumber, VideoNumber, FileSize, TileId, UserNumber FROM data003 ORDER BY segNumber ASC, UserNumber ASC;"
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()

    return [
        {"segNumber": row[0], "videoNumber": row[1], "fileSize": row[2], "tileId": row[3]}
        for row in results
    ]

# ================= LFU 캐시 사용 =================
cache_capacity_bytes = 900000000  # 캐시 최대 크기 (765,489,791 bytes)  459293874
lfu_cache = LFUCache(capacity_bytes=cache_capacity_bytes)

# MySQL 데이터 로드 후 캐시에 저장
data_list = load_data_from_mysql()


for data in data_list:
   lfu_cache.put(data["segNumber"], data["videoNumber"], data["tileId"], data, data["fileSize"])
    
     
hit_rate = (lfu_cache.hit/119616 )*100
print('cache hit : '+ str(lfu_cache.hit)+ ' cache miss : '+ str(lfu_cache.miss)+ ' hit rate : '+ str(hit_rate))



#print(f"❌ LFU 제거: Segment={lfu_key[0]}, Video={lfu_key[1]}, TileID={lfu_key[2]}, Size={removed_size} bytes, Frequency={min_freq}")
