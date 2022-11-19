

import os
import time
import requests
from lxml import etree
from bs4 import BeautifulSoup
import urllib.parse
from kafka import KafkaProducer
import json
from snownlp import SnowNLP
import re

from weibo_top_sentiment_bayes import *

bayesSentimentModel = BayesSentiment()

headers={
    'Host':'s.weibo.com',
    'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding':'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Referer': 'https://weibo.com/',
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.42',
    'cookie':'SINAGLOBAL=364328318699.485.1655211514565; UOR=,,hi.kejiwanjia.com; _s_tentry=-; Apache=3605892075168.0425.1668847355954; ULV=1668847356614:4:2:1:3605892075168.0425.1668847355954:1667910647412; SUB=_2A25OfNtVDeRhGeNJ7lsR-CbFzz2IHXVtCEudrDV8PUNbmtAKLVLXkW9NS66eQESrTZKjTUf_n2Fg8XB6Y09k5C1_; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWEur1sYEmmOv27V4vD0SFF5JpX5KzhUgL.Fo-NSK.71hn4Sh22dJLoIX.LxKBLBonL1h5LxK-L12qLB-2LxKqLBozLBK2LxKqL1-eL1h.LxK.L1KzLBo2LxKML1-2L1hBLxK-L1KzLBonLxK.LBo2LB.Sk; ALF=1700388484; SSOLoginState=1668852485'
}

def get_top(producer):
    url = "https://s.weibo.com/top/summary"

    r = requests.get(url,headers=headers)
    print(r.status_code)

    html_xpath = etree.HTML(r.text)
    data = html_xpath.xpath('//*[@id="pl_top_realtimehot"]/table/tbody/tr/td[2]')
    num = 1
    for tr in (data):
        print('---------------------')
        title = tr.xpath('./a/text()')
        hot_score = tr.xpath('./span/text()')
        href = tr.xpath('./a/@href')
        # 有热度值的是热搜标题，否则可能是广告推广
        if hot_score:
            print('{} {} hot: {}'.format(num, title[0], hot_score[0]))
            result = get_weibo_list('https://s.weibo.com/weibo?q=%23'+title[0]+'%23&Refer=top')
            for each in result:
                sentiment_score = bayesSentimentModel.predict(each)
                print(title[0], sentiment_score)
                msg = {"title":title[0], "time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "sentiment_score": sentiment_score}
                producer.send('weibotop', key='ff', value=json.dumps(msg))
            num += 1
        time.sleep(2)

def get_weibo_list(url):
    r = requests.get(url,headers=headers)

    bs = BeautifulSoup(r.text)
    body = bs.body
    div_m_main = body.find('div', attrs={'class': 'm-main'})
    div_m_wrap = div_m_main.find('div', attrs={'class':'m-wrap'})
    div_m_con_l = div_m_wrap.find('div', attrs={'class':'m-con-l'})
    data_div = div_m_con_l.findAll('div', attrs={'class':'card-wrap', 'action-type':'feed_list_item'})

    weibo_list = []
    for each_div in data_div:
        div_card = each_div.find('div', attrs={'class':'card'})
        div_card_feed = div_card.find('div', attrs={'class':'card-feed'})
        div_content = div_card_feed.find('div', attrs={'class':'content'})

        p_feed_list_content = div_content.find('p', attrs={'class':'txt','node-type':'feed_list_content'})
        content_text = p_feed_list_content.get_text()

        p_feed_list_content_full = div_content.find('p', attrs={'class':'txt','node-type':'feed_list_content_full'})
        if p_feed_list_content_full:
            content_text = p_feed_list_content_full.get_text()

        weibo_list.append(content_text.strip())

    return weibo_list



producer = KafkaProducer(bootstrap_servers='localhost:9092',
    key_serializer=lambda k: json.dumps(k).encode(), 
    value_serializer=lambda v: json.dumps(v).encode())

while True:
    get_top(producer=producer)
    time.sleep(600)

            
