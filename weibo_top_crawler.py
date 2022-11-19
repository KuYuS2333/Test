

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

def sentiment_analysis(sentence):
    total = 0
    count = 0
    li = re.split("[。！？]", sentence)
    for each in li:
        each = each.strip()
        if each:
            s = SnowNLP(each)
            total += s.sentiments
            count += 1

    if count == 0:
        return 0.5
    else:
        return total / count

def get_top2():
    url = "https://s.weibo.com/top/summary"
    headers={
        'Host': 's.weibo.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://weibo.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
    }
    r = requests.get(url,headers=headers)
    print(r.status_code)
    html_xpath = etree.HTML(r.text)
    data = html_xpath.xpath('//*[@id="pl_top_realtimehot"]/table/tbody/tr/td[2]')
    num = -1
    for tr in (data):
        title = tr.xpath('./a/text()')
        hot_score = tr.xpath('./span/text()')
        href = tr.xpath('./a/@href')
        num += 1
        # 过滤第 0 条
        if num == 0:
            pass
        else:
            if hot_score:
                #print(num,title[0],'微博此时的热度为：',hot_score[0])
                result = get_subtop('https://s.weibo.com/weibo?q=%23'+title[0]+'%23&Refer=top')
                totalscore = 0
                count = 0
                for each in result:
                    sentiment_score = bayesSentimentModel.predict(each)
                    totalscore += sentiment_score
                    count += 1
                if count > 0:
                    print(title[0], totalscore/count)
        time.sleep(2)

def get_top():
    url = "https://s.weibo.com/top/summary"
    headers={
        'Host': 's.weibo.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://weibo.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
    }
    r = requests.get(url,headers=headers)
    print(r.status_code)
    html_xpath = etree.HTML(r.text)
    data = html_xpath.xpath('//*[@id="pl_top_realtimehot"]/table/tbody/tr/td[2]')
    num = -1
    for tr in (data):
        title = tr.xpath('./a/text()')
        hot_score = tr.xpath('./span/text()')
        href = tr.xpath('./a/@href')
        num += 1
        # 过滤第 0 条
        if num == 0:
            pass
        else:
            if hot_score:
                print(num,title[0],'微博此时的热度为：',hot_score[0])
                result = get_subtop('https://s.weibo.com/weibo?q=%23'+title[0]+'%23&Refer=top')
                for each in result:
                    sentiment_score = bayesSentimentModel.predict(each)
                    print(title[0], sentiment_score)
                    msg = {"title":title[0], "time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "sentiment_score": sentiment_score}
            time.sleep(2)



url2 = 'https://s.weibo.com/weibo?q=%23%E7%A1%AE%E4%BF%9D%E9%87%8D%E8%A6%81%E6%B0%91%E7%94%9F%E5%95%86%E5%93%81%E4%BF%9D%E4%BE%9B%E7%A8%B3%E4%BB%B7%23&Refer=new_time'
def get_subtop(url=url2):
    data_detail_xpath = '//*[@id="pl_feedlist_index"]'
    headers={
        'Host': 's.weibo.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://weibo.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
    }
    r = requests.get(url,headers=headers)
    # print(r.status_code)
    # html_xpath = etree.HTML(r.text)
    # print(r.text)
    # data2 = html_xpath.xpath(data_detail_xpath)
    # print(data2)

    bs = BeautifulSoup(r.text)
    body = bs.body
    div_m_main = body.find('div', attrs={'class': 'm-main'})
    div_m_wrap = div_m_main.find('div', attrs={'class':'m-wrap'})
    div_m_con_l = div_m_wrap.find('div', attrs={'class':'m-con-l'})
    data_div = div_m_con_l.findAll('div', attrs={'class':'card-wrap', 'action-type':'feed_list_item'})

    top_list = []
    for each_div in data_div:
        #print('################')
        #print(each_div)
        # div_card = each_div.find('div', attrs={'class':'card'})
        # if div_card:
        #     div_card_feed = div_card.find('div', attrs={'class':'card-feed'})
        #     div_content = div_card_feed.find('div', attrs={'class':'content'})
        #     p_feed_list_content = div_content.find('p', attrs={'note-type':'feed_list_content'})
        #     print(p_feed_list_content.text)
        div_card = each_div.find('div', attrs={'class':'card'})
        div_card_feed = div_card.find('div', attrs={'class':'card-feed'})
        div_content = div_card_feed.find('div', attrs={'class':'content'})

        p_feed_list_content = div_content.find('p', attrs={'class':'txt','node-type':'feed_list_content'})
        #print(div_content.prettify())
        #print(p_feed_list_content.get_text())
        content_text = p_feed_list_content.get_text()

        p_feed_list_content_full = div_content.find('p', attrs={'class':'txt','node-type':'feed_list_content_full'})
        if p_feed_list_content_full:
            #print(p_feed_list_content_full.get_text())
            content_text = p_feed_list_content_full.get_text()

        top_list.append(content_text.strip())
        #p_feed_list_content = div_content.find('p', attrs={'note-type':'feed_list_content'})
        #print(p_feed_list_content.text)


    return top_list




#get_top()


while True:
    get_top2()

    time.sleep(600)



            
