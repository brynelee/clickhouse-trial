# 插入测试数据

from clickhouse_driver import Client
from datetime import datetime, timedelta, timezone


client = Client(host='spark-master', port='9500', user='hadoop', password='spark-master', database='indexsysdb')
insert_keyword1h = 'insert into keyword1h (keyword,dtime,source1,source2,mood,category,amount) VALUES'

tz_utc_8 = timezone(timedelta(hours=8)) # 创建时区UTC+8:00

data = [
{'source1': 'video', 'source2': 'youtube', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'}, 
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'},
{'source1': 'overseas', 'source2': 'facebook', 'mood': 3, 'category': 4, 'amount': 1, 'keyword': '宋洋葱'}]
tm = datetime.now()
dtime = datetime(tm.year, tm.month, tm.day, tm.hour,tzinfo=tz_utc_8)
rows = []
for x in data:
	x['dtime'] = dtime
	rows.append(x)

client.execute(insert_keyword1h,rows)

