# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql, json, time, os, pickle
from tiantianfondsSearch.items import BasicinfoItem, StocksinfoItem, BondinfoItem

class TiantianfondssearchPipeline(object):
    def __init__(self):
        self.db = pymysql.connect('localhost','root','root1234','funds_info')
        self.cur = self.db.cursor()
        self.exists_1 = False
        self.exists_2 = False
        self.exists_3 = False

    def create_table(self, tablename, keys):
        sql = """create table if not exists %s (id int not null auto_increment, {}, primary key(id));"""%tablename
        inside = ' varchar(30) not null default " ",'.join(keys) +  ' varchar(30) not null default " "'
        sql = sql.format(inside)
        self.cur.execute(sql)
        self.cur.connection.commit()

    def insert_values(self, tablename, keys, values):
        sql = "insert into %s({})"%tablename
        sql = sql.format(', '.join(keys))
        sql += """ values{};""".format(values)
        self.cur.execute(sql)
        self.cur.connection.commit()

    def process_item(self, item, spider):
        if isinstance(item, BasicinfoItem):
            print('基金概况写入数据库...')
            keys_in_item = list(item.get('basicinfo').keys())
            keys_in_item = ['`'+_+'`' for _ in keys_in_item]
            if not self.exists_1:
                self.create_table('basicinfo', keys_in_item)
                self.exists_1 = True
            values = list(item.get('basicinfo').values())
            values = ["'"+str(_)+"'" for _ in values]
            values = ', '.join(values)
            values = '(' + values +')'
            self.insert_values('basicinfo', keys_in_item, values)
            return item
        elif isinstance(item, StocksinfoItem):
            print('基金持仓情况写入数据库...')
            keys_in_item = list(item.get('stocksinfo')[0].keys())
            keys_in_item = ['`'+_+'`' for _ in keys_in_item]
            if not self.exists_2:
                self.create_table('stocksinfo', keys_in_item)
                self.exists_2 = True
            values = item.get('stocksinfo')
            values = [list(_.values()) for _ in values]
            new = []
            for value in values:
                temp = tuple([str(_) for _ in value])
                new.append(str(temp))
            values = ', '.join(new)
            self.insert_values('stocksinfo', keys_in_item, values)
            return item
        elif isinstance(item, BondinfoItem):
            print('基金债券持仓写入数据库...')
            keys_in_item = list(item.get('bondinfo')[0].keys())
            keys_in_item = ['`'+_+'`' for _ in keys_in_item]
            if not self.exists_3:
                self.create_table('bondinfo', keys_in_item)
                self.exists_3 = True
            values = item.get('bondinfo')
            values = [list(_.values()) for _ in values]
            new = []
            for value in values:
                temp = tuple([str(_) for _ in value])
                new.append(str(temp))
            values = ', '.join(new)
            self.insert_values('bondinfo', keys_in_item, values)
            return item
        else:
            print("""\n\n\n肯定哪里出错了！！！！\n\n\n""")
            return item

    def close_spider(self,spider):
        self.db.close()

class TiantianfondssearchPipeline2(object):
    def __init__(self):
        self.timestamp = str(round(time.time(),1))
        self.data_jbgk = open(self.timestamp + '_jbgk.json','w',encoding='utf-8')
        self.data_jjcc = open(self.timestamp + '_jjcc.json','w',encoding='utf-8')
        self.data_zqcc = open(self.timestamp + '_zqcc.json','w',encoding='utf-8')

    def open_spider(self, spider):
        self.id = spider.id
        self.fundcodes4pipeline = spider.fundcodes4pipeline
        self.filename = spider.filename

    def filt_data(self, item):
        criteria = item.get('fundcode')
        try:
            self.fundcodes4pipeline.pop(criteria)
        except KeyError as e:
            pass

    def process_item(self,item, spider):
        if isinstance(item, BasicinfoItem):
            content = json.dumps(item.get('basicinfo'))+'\n'
            self.data_jbgk.write(content)
            self.filt_data(item)
            return item
        elif isinstance(item, StocksinfoItem):
            content = json.dumps(item.get('stocksinfo'))+'\n'
            self.data_jjcc.write(content)
            self.filt_data(item)
            return item
        elif isinstance(item, BondinfoItem):
            content = json.dumps(item.get('bondinfo'))+'\n'
            self.data_zqcc.write(content)
            self.filt_data(item)
            return item
        else:
            return item

    def rename(self, oldname,newname):
        try:
            os.rename(oldname, newname)
        except FileExistsError as e:
            os.remove(newname)
            os.rename(oldname, newname)

    def close_spider(self, spider):
        self.data_jbgk.close()
        self.data_jjcc.close()
        self.data_zqcc.close()
        self.rename(self.timestamp + '_jbgk.json', str(self.id) + '_jbgk.json')
        self.rename(self.timestamp + '_jjcc.json', str(self.id) + '_jjcc.json')
        self.rename(self.timestamp + '_zqcc.json', str(self.id) + '_zqcc.json')
        if len(self.fundcodes4pipeline) == 0:
            try:
                os.remove(self.filename)
            except Exception:
                print('file already removed')
        else:
            with open(self.filename, 'wb') as f:
                pickle.dump(self.fundcodes4pipeline, f)
