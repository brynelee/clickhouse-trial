# -*- coding: utf-8 -*-
'''
Created on 2021年3月1日

@author: copy from others
'''
import pandas as pd
import json
import numpy as np
import datetime
from clickhouse_driver import Client
#from clickhouse_driver import connect

# 基于Clickhouse数据库基础数据对象类
class DB_Obj(object):
    '''
    spark-master:9500
    ebd_all_b04.card_tbl_trade_m_orc
    '''
    def __init__(self, db_name):
        self.db_name = db_name
        host='spark-master' #服务器地址
        port ='9500' #'8123' #端口
        user='hadoop' #用户名
        password='spark-master' #密码
        database=db_name #数据库
        send_receive_timeout = 25 #超时时间
        self.client = Client(host=host, port=port, database=database) #, send_receive_timeout=send_receive_timeout)
        #self.conn = connect(host=host, port=port, database=database) #, send_receive_timeout=send_receive_timeout)
        
    def setPriceTable(self,df):
        self.pricetable = df

    def get_trade(self,df_trade,filename):          
        print('Trade join price!')
        df_trade = pd.merge(left=df_trade,right=self.pricetable[['occurday','DIM_DATE','END_DATE','V_0','V_92','V_95','ZDE_0','ZDE_92',
                              'ZDE_95']],how="left",on=['occurday'])

        df_trade.to_csv(filename,mode='a',encoding='utf-8',index=False)

    def get_datas(self,query_sql):          
        n = 0 # 累计处理卡客户数据
        k = 0 # 取每次DataFrame数据量
        batch = 100000 #100000 # 分批次处理
        i = 0 # 文件标题顺序累加
        flag=True # 数据处理解释标志
        filename = 'card_trade_all_{}.csv'
        while flag:
            self.query_sql = query_sql.format(n, n+batch) 
            print('query started')
            collection = self.client.query_dataframe(self.query_sql)
            print('return query result')
            df_trade = collection #pd.DataFrame(collection)
            
            i=i+1
            k = len(df_trade) 
            if k > 0:
                self.get_trade(df_trade, filename.format(i))
            
            n = n + batch
            if k == 0:
                flag=False        
            print('Completed ' + str(k) + 'trade details!')
            print('Usercard count ' + str(n) )    
               
        return n                

# 价格变动数据集
class Price_Table(object):
    def __init__(self, cityname, startdate):
        self.cityname = cityname
        self.startdate = startdate
        self.filename = 'price20210531.csv'
        
    def get_price(self):
        df_price = pd.read_csv(self.filename)
        ......

            self.price_table=self.price_table.append(data_dict, ignore_index=True)    
            
        print('generate price table!')   

class CardTradeDB(object):
    def __init__(self,db_obj): 
        self.db_obj = db_obj
        
    def insertDatasByCSV(self,filename):
        # 存在数据混合类型
        df = pd.read_csv(filename,low_memory=False)
        
    # 获取交易记录    
    def getTradeDatasByID(self,ID_list=None):
        # 字符串过长，需要使用'''
        query_sql = '''select C.carduser_id,C.org_id,C.cardasn,C.occurday as 
        		......
                limit {},{})
                group by C.carduser_id,C.org_id,C.cardasn,C.occurday
                order by C.carduser_id,C.occurday'''
        
        
        n = self.db_obj.get_datas(query_sql)
        
        return n
                    
if __name__ == '__main__':
    PTable = Price_Table('湖北','2015-12-01')   
    PTable.get_price()  
    
    db_obj = DB_Obj('ebd_all_b04')
    db_obj.setPriceTable(PTable.price_table)
    CTD = CardTradeDB(db_obj)
    df = CTD.getTradeDatasByID()
