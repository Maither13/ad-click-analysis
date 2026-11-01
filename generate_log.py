import random
import time
from datetime import datetime

ads = ["ad_001", "ad_002", "ad_003", "ad_004", "ad_005", "ad_006", "ad_007", "ad_008"]
provinces = ["广东省", "浙江省", "江苏省", "湖北省", "四川省"]
city_map = {
    "广东省": "深圳市",
    "浙江省": "杭州市",
    "江苏省": "南京市",
    "湖北省": "武汉市",
    "四川省": "成都市"
}

log_path = "/opt/logs/ad_click.log"

if __name__ == "__main__":
    f = open(log_path, "a")
    while True:
        ad = random.choice(ads)
        province = random.choice(provinces)
        city = city_map[province]
        user = "user_" + str(random.randint(1000, 9999))
        t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        line = t + "," + ad + "," + province + "," + city + "," + user
        print(line)
        f.write(line + "\n")
        f.flush()
        
        time.sleep(0.5)
