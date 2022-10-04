import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
from fake_useragent import UserAgent

def labe_find(str_find):
    for p in range(len(paging.find_all('span',class_='list__label'))):
        if str_find in paging.find_all('span',class_='list__label')[p].text.strip():
            return paging.find_all('span',class_='list__content')[p].text.strip()
            
item = 1
url = 'https://www.rakuya.com.tw/sell/result?city=8&sort=11&browsed=0&page='+str(item)

user_agent = UserAgent(use_cache_server=False)
m_header = user_agent.random

headers = {"User-Agent": m_header,
           "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
           "Accept-Encoding": "none",
           "Accept-Language": "en-US,en;q=0.8",
           "Connection": "keep-alive",
           "content-Type":"application/json"}
request = requests.get(url, headers=headers)
web2 = BeautifulSoup(request.text,'html5lib')
total_page = int(web2.find('p',class_='pages').text.split('/')[1][:-1].strip())

all_url=[]
for item in range(1):
    for web in web2.find_all('a', class_='browseItemDetail'):
        all_url.append(web.get('href'))
    item+=1
    delay_choice = [1,3,5,7,9,20]
    delay = random.choice(delay_choice)
    time.sleep(delay)
    
    m_header = user_agent.random
    headers = {"User-Agent":m_header}
    request = requests.get(url,headers=headers)
    web2 = BeautifulSoup(request.text,'html5lib')

print('url dowload complete')
word = ['開','放','式','格','局','-','']
rakuya=[]
for p_url in all_url:
    try:
        request = requests.get(p_url,headers=headers, timeout = 5)
    except:
        pass
    paging = BeautifulSoup(request.text,'html5lib')
    rakuya.append({
                    '標題': paging.find('span',class_='title').text.strip(),            
                    '建案座落位置': paging.find('h1',class_='txt__address').text.strip(),
                    '建物': labe_find('建物登記'),
                    '主建物': labe_find('主建物'),
                    '附屬建物': labe_find('附屬建物'),
                    '公共設施' : labe_find('公共設施'),
                    '土地登記' : labe_find('土地登記'),
                    '房間': None if paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[0:1] in word else paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[0:1],
                    '廳堂': None if paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[2:3] in word else paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[2:3],
                    '衛浴': None if paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[4:5] in word else paging.find_all('ul',class_='list__info')[0].find_all('li')[0].get_text()[4:5],
                    '年份': paging.find_all('ul',class_='list__info')[0].find_all('li')[2].get_text()[:-3],
                    '樓層':labe_find('樓層'),
                    '車位與否':paging.find_all('div',class_='list__info-sub')[2].find('span',class_='list__content').text.strip(),
                    '車位類型':paging.find_all('ul',class_='group')[2].find_all('span',class_='list__content')[1].text.strip()if paging.find_all('div',class_='list__info-sub')[2].find('span',class_='list__content').text.strip()=='有車位' else None,
                    '開價': paging.find('span',class_='txt__price-total').text[:-1].strip(),
                    '特色描述' : paging.find('div',class_='block__info-detail').find('div',class_='content').text.strip().replace(u'\xa0',u'') if paging.find('div',class_='block__info-detail').find('div',class_='content') else None
            })
    request.close()
    delay_choice = [1,3,5,7,9,20]
    delay = random.choice(delay_choice)
    time.sleep(delay)

df = pd.DataFrame(rakuya)

#存進mongo內的airflow的rakuya(collection)
from airflow.providers.mongo.hooks.mongo import MongoHook
hook = MongoHook(conn_id="mongo_default")
collection = hook.get_collection("rakuya", "airflow")
records = df.to_dict('records') 
collection.insert_many(records)

