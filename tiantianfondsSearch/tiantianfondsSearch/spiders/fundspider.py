import scrapy, pickle
from tiantianfondsSearch.items import BasicinfoItem, StocksinfoItem, BondinfoItem

class fundinfoSpider(scrapy.Spider):
    name = "fundspider"
    # first, we have to get raw data, that is the complete fundcodes, from tiantian fund website
    # then pickle it as a file
    # I skipped that step here
    # "id" and "file" shall be passed in console when starting spider
    # use the command: scrapy crawl fundspider -a file=filename -a id=id_num
    def __init__(self, file="fundcodesData.pl", id=None):
        with open(str(file),'rb') as f:
            fundcodes = pickle.load(f)
            # depends on what data structure are you using, I use dict
            if isinstance(fundcodes, list):
                fundcodes = dict(map(lambda x:(x[0],x),fundcodes))
        self.id = id
        self.filename = file
        self.fundcodes = fundcodes
        self.fundcodes4pipeline = fundcodes.copy()
        self.index = 0
        self.jbgk = "http://fundf10.eastmoney.com/jbgk_{}.html"
        self.jjcc = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10"
        self.zqcc = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=zqcc&code={}"
        self.fundname = ''
        self.fundtype = ''
        self.fundcode = ''

    def start_requests(self):
        for k, each_fund in self.fundcodes.items():
            self.fundcode = k
            self.fundtype = each_fund[3]
            self.fundname = each_fund[2]
            jbgk_url = self.jbgk.format(self.fundcode)
            jjcc_url = self.jjcc.format(self.fundcode)
            zqcc_url = self.zqcc.format(self.fundcode)
            print('\n获取基金 [%s] 详情......\n'%self.fundname)
            yield scrapy.Request(url=jbgk_url, callback=self.get_jbgk)
            yield scrapy.Request(url=jjcc_url, callback=self.get_jjcc)
            yield scrapy.Request(url=zqcc_url, callback=self.get_zqcc)

    def get_jbgk(self, response):
        print('%s 基本概况'%self.fundname)
        item = BasicinfoItem()
        table = response.xpath('//div[@class="box"][1]//table')
        date = table.xpath('.//tr[3]//td[1]/text()').get()
        scale = table.xpath('.//tr[4]//td[1]/text()').get()
        managing_institute = table.xpath('.//tr[5]//td[1]//text()').get()
        manager = table.xpath('.//tr[6]//td[1]//text()').get()
        trusted_by = table.xpath('.//tr[5]//td[2]//text()').get()
        share = table.xpath('.//tr[6]//td[2]//text()').get()
        item['basicinfo'] = {'基金代码':self.fundcode,
                             '基金名称':self.fundname,
                             '基金类型':self.fundtype,
                             '发行日期':date,
                             '基金规模':scale,
                             '管理机构':managing_institute,
                             '基金经理人':manager,
                             '基金托管人':trusted_by,
                             '成立来分红':share}
        item['fundcode'] = self.fundcode
        # print("*"*30)
        yield item

    def get_jjcc(self, response):
        print('%s 股票持仓信息'%self.fundname)
        def get_details(content):
            titles = ['序号','股票代码','股票名称','最新价','涨跌幅','相关资讯','占净值比例','持股数（万股）','持仓市值（万元）']
            res = {}
            for i in enumerate(titles):
                text = content.xpath('.//td[%d]//text()'%(i[0]+1)).getall()
                text = [str(_) for _ in text]
                text = ''.join(text)
                res[i[1]] = text
            return res

        item = StocksinfoItem()
        try:
            content = response.xpath('//div[@class="box"][1]//table//tbody//tr[1]')
            final = []
            while content:
                singlestock = get_details(content)
                singlestock['基金代码'] = self.fundcode
                final.append(singlestock)
                content = content.xpath('./following-sibling::tr[1]')
            item['stocksinfo'] = final
            if not item['stocksinfo']:
                raise Exception
            item['fundcode'] = self.fundcode
            yield item
        except:
            empty = [' ' for i in range(9)]
            titles = ['序号','股票代码','股票名称','最新价','涨跌幅','相关资讯','占净值比例','持股数（万股）','持仓市值（万元）']
            temp = dict(zip(titles, empty))
            temp['基金代码'] = self.fundcode
            item['stocksinfo'] = [temp,]
            item['fundcode'] = self.fundcode
            yield item

    def get_zqcc(self, response):
        print('%s 债券持仓信息'%self.fundname)
        def get_details(content):
            titles = ['序号','债券代码','债券名称','占净值比例','持仓市值（万元）']
            res = {}
            for i in enumerate(titles):
                text = content.xpath('.//td[%d]//text()'%(i[0]+1)).getall()
                text = [str(_) for _ in text]
                text = ''.join(text)
                res[i[1]] = text
            return res

        item = BondinfoItem()
        try:
            content = response.xpath('//div[@class="box"][1]//table//tbody//tr[1]')
            final = []
            while content:
                singlestock = get_details(content)
                singlestock['基金代码'] = self.fundcode
                final.append(singlestock)
                content = content.xpath('./following-sibling::tr[1]')
            item['bondinfo'] = final
            if not item['bondinfo']:
                raise Exception
            item['fundcode'] = self.fundcode
            yield item
        except:
            titles = ['序号','债券代码','债券名称','占净值比例','持仓市值（万元）']
            empty = [' ' for i in range(5)]
            temp = dict(zip(titles, empty))
            temp['基金代码'] = self.fundcode
            item['bondinfo'] = [temp,]
            item['fundcode'] = self.fundcode
            yield item
