import urllib.request
from bs4 import BeautifulSoup
import time
import http.cookiejar
import csv
import requests
import random
import threading


with open('dianping_page.csv','w',
          encoding='utf-16',
          newline='') as csvfile:
    field_header = ['dianping_url','shop_name','shop_city','shop_district_1','shop_district_2','shop_district_3','shop_address',
              'shop_tel','shop_hours','shop_comment_num','shop_avg_price','shop_rank','shop_rank_1','shop_rank_2',
             'shop_rank_3','shop_rank_4','shop_intro','shop_nick_name', 'shop_lat','shop_lng']
    fp_writer = csv.writer(csvfile,dialect='excel',delimiter='|')
    fp_writer.writerow(field_header)

browser_user_agent_list = [
'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0 ',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36 ',
'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E) ',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.124 Safari/537.36 ',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; vivo X1 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 MicroMessenger/4.3.219 ',
'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
'Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12F70 MicroMessenger/6.3.13 NetType/WIFI Language/zh_CN',
'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E) ',
'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0); 360Spider',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; TCL S820 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.1 Mobile Safari/534.30 MicroMessenger/4.2.192',
'Mozilla/5.0 (Linux; U; Android 5.1.1; zh-CN; G750-T01 Build/LMY49G) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.10.3.810 U3/0.8.0 Mobile Safari/534.30',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36 ',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; SCH-N719 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 MicroMessenger/5.0.2.352',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
]

# generate random header
def gen_rand_header():
    rand_ua = random.choice(browser_user_agent_list)
    rand_header = {'User-Agent':rand_ua,
                   'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                   'Accept-Language':'en-US,en;q=0.5',
                   'Accept-Encoding':'gzip, deflate, sdch',
                   'Connection':'keep-alive',
                   'Referer':'http://www.kuaidaili.com/proxylist/1/'}
    return rand_header

# Connection retry
def url_connect_retry(i_url, i_head = gen_rand_header(),
    i_param = '',
    retry_num = 3):
    '''

    :param i_url:
    :param retry_num:
    :return: request object
    '''
    try:
        resp = requests.get(i_url, headers = i_head, params=i_param, timeout=5)
        return resp
    except Exception as e:
        if retry_num > 0:
            # time.sleep(5)
            print("%s\n%s retry left" %(i_url, retry_num))
            return url_connect_retry(i_url, i_head,'',retry_num-1)
        else:
            print([e, 'Connection Issue'])
            with open('dianping_page_error.csv', 'a',encoding='utf-16', newline='') as csvfile:
                fp_writer = csv.writer(csvfile,dialect='excel', delimiter='|')
                fp_writer.writerow([i_url,e])
            return ''

def webpages_get_details(bs_content):
    shop_name= ''
    shop_city= ''
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:

        try:
            scripts = bs_content.find_all('script')
            for script in scripts:
                scriptStr=script.text

                if "lng:" in scriptStr:

                    location=scriptStr[scriptStr.index("lng:"):scriptStr.index("});")]
                    shop_lng =location[location.index(":")+1:location.index(",")]
                    shop_lat =location[location.index(",")+5:]
        except:
            pass
        try:

            shop_name = bs_content.find('div',attrs={'class':'main'}).h1.next_element.replace('\n','').replace('|','').strip()
        except:
            pass

        try:
            shop_city = bs_content.find('a',attrs= {'class':'city J-city'}).get_text().replace('\n','').replace('|','').strip()
        except:
            pass
        try:
            shop_district_1 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.next_element.replace('\n','').replace('|','').strip()
        except:
            pass
        try:
            shop_district_2 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next().get_text().replace('\n','').replace('|','').strip()
        except:
            pass
        try:
            shop_district_3 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next().find_next().get_text().replace('\n','').replace('|','').strip()
        except:
            pass
        try:
            shop_address = bs_content.find('div',attrs={'class':'expand-info address'}).find('span',attrs ={'class':'item'}).get_text().replace('\n','').replace('|','').strip()
        except:
            pass
        try:
            shop_tel = str(bs_content.find('p',attrs={'class':'expand-info tel'}).find(class_='item').get_text().replace('\n','').replace('|','').strip())
        except:
            pass
        shop_infor_list_p = []
        try:
            shop_infor_list = bs_content.find(class_='other J-other Hide').find_all('p')
            for shop_infor in shop_infor_list:
                shop_infor_list_p.append(shop_infor.get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('\xa0',''))
            for infor_item in shop_infor_list_p:
                if '营业时间' in infor_item:
                    shop_hours = infor_item.replace('修改','')
                elif '简介' in infor_item:
                    shop_intro = infor_item
                elif '别名' in infor_item:
                    shop_nick_name = infor_item
        except:
            pass

        shop_comment_list = bs_content.find('div',attrs={'class':'brief-info'}).find_all('span')
        try:
            shop_rank = shop_comment_list[0]['title'].replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_rank)
        except:
            pass
        try:
            shop_comment_pool = []
            for shop_comment in shop_comment_list[1:]:
                shop_comment_pool.append(shop_comment.get_text().replace('\n', '').replace('|', '').replace(' ', ''))
            # print(shop_comment_pool)
            for comment_item in shop_comment_pool:
                if '评论' in comment_item or '体验报告' in comment_item:
                    shop_comment_num = comment_item
                    # print('评论是%s'% shop_comment_num)
                    continue
                elif '人均' in comment_item or '消费' in comment_item or '花费' in comment_item or '均价' in comment_item or '价格' in comment_item or '费用' in comment_item:
                    shop_avg_price = comment_item
                    # print('人均消费是 %s' % shop_avg_price)
        except:
            pass
        try:
            shop_comment_pool.remove(shop_comment_num)
        except:
            pass
        try:
            shop_comment_pool.remove(shop_avg_price)
        except:
            pass
        try:
            if len(shop_comment_pool) > 3:
                shop_rank_1 = shop_comment_pool[0]
                shop_rank_2 = shop_comment_pool[1]
                shop_rank_3 = shop_comment_pool[2]
                shop_rank_4 = shop_comment_pool[3]
            elif len(shop_comment_pool) == 3:
                shop_rank_1 = shop_comment_pool[0]
                shop_rank_2 = shop_comment_pool[1]
                shop_rank_3 = shop_comment_pool[2]
            elif len(shop_comment_pool) == 2:
                shop_rank_1 = shop_comment_pool[0]
                shop_rank_2 = shop_comment_pool[1]
            elif len(shop_comment_pool) == 1:
                shop_rank_1 = shop_comment_pool[0]
            else:
                pass
            # print('shop_rank_1 is %s,shop_rank_2 is %s,shop_rank_3 is %s' % (shop_rank_1,shop_rank_2,shop_rank_3))
        except:
            pass
    except:
        pass


    record_item = [dianping_url,shop_name,shop_city,shop_district_1,shop_district_2,shop_district_3,shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]

    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        # print(record_item)
        with open('dianping_page.csv', 'a',encoding='utf-16', newline='') as csvfile:
            fp_writer = csv.writer(csvfile,dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)



def webpages_kids_get_details(bs_content):
    shop_name= ''
    shop_city= '',
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_district_4 = ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:

        try:

            shop_name = bs_content.find('div', attrs={'class':'shop-name'}).h1.next_element.replace('\n','').replace('|','').strip()
            # print(shop_name)
        except:
            pass


        try:
            shop_city = bs_content.find('a',attrs= {'class':'city J-city'}).get_text().replace('\n','').replace('|','').strip()
            # print(shop_city)
        except:
            pass

        try:
            shop_district_1 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_1)
        except:
            pass
        try:
            shop_district_2 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_2)
        except:
            pass
        try:
            shop_district_3 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_3)
        except:
            pass
        try:
            shop_district_4 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_4)
        except:
            pass
        try:
            shop_address = bs_content.find('div',attrs={'class':'shop-addr'}).find('span')['title'].replace('\n','').replace('|','').strip()
            # print(shop_address)
        except:
            pass
        try:
            shop_tel = str(bs_content.find('div',attrs={'class':'shopinfor'}).find('p').find('span').get_text().replace('\n','').replace('|','').strip())
            # print(shop_tel)
        except:
            pass
        shop_infor_list_p = []
        try:
            shop_infor_list = bs_content.find(class_='more-class').find_all('p')
            for shop_infor in shop_infor_list:
                shop_infor_list_p.append(shop_infor.get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('\xa0',''))
            for infor_item in shop_infor_list_p:
                if '营业时间' in infor_item:
                    shop_hours = infor_item.replace('修改','')
                elif '简介' in infor_item:
                    shop_intro = infor_item
                elif '别名' in infor_item:
                    shop_nick_name = infor_item
            # print(shop_hours)
        except:
            pass
        try:
            shop_comment_list = bs_content.find('div', attrs={'class': 'comment-rst'}).find_all('span')
            # print(shop_comment_list)
        except:
            pass

        try:
            shop_rank = shop_comment_list[0].get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('\r','')
        except:
            pass
        try:
            shop_comment_num = shop_comment_list[1].get_text().replace('\n', '').replace('|', '').replace(' ', '')+'封点评'
        except:
            pass
        try:
            shop_avg_price = shop_comment_list[2].get_text().replace('\n', '').replace('|', '').replace(' ', '')
        except:
            pass

    except:
        pass
    record_item = [dianping_url, shop_name, shop_city, shop_district_1, shop_district_2, shop_district_3, shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]
    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        # print(record_item)
        with open('dianping_page.csv', 'a',encoding='utf-16', newline='') as csvfile:
            fp_writer = csv.writer(csvfile,dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)

    else:
        pass

def webpages_housing_get_details(bs_content):
    shop_name= ''
    shop_city= '',
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_district_4 = ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:

        try:

            shop_name = bs_content.find('div', attrs={'class':'shop-name'}).h1.next_element.replace('\n','').replace('|','').strip()
            # print(shop_name)
        except:
            pass


        try:
            shop_city = bs_content.find('div', attrs={'class':'header'}).find('span', attrs ={'class':'txt'}).get_text().replace('\n','').replace('|','').strip().replace('站','')
            # print(shop_city)
        except:
            pass

        try:
            shop_district_1 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_1)
        except:
            pass
        try:
            shop_district_2 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_2)
        except:
            pass
        try:
            shop_district_3 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_3)
        except:
            pass
        try:
            shop_district_4 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_4)
        except:
            pass



        try:
            shop_address = bs_content.find('div',attrs={'class':'desc-list'}).find('dt').get_text() +bs_content.find('div',attrs={'class':'desc-list'}).find('span',attrs = {'itemprop':'street-address'}).get_text().replace('\n','').replace('|','').strip()
            # print(shop_address)
        except:
            pass
        try:
            shop_tel = str(bs_content.find('div',attrs={'class':'desc-list'}).find('strong',attrs = {'itemprop':'tel'}).get_text().replace('\n','').replace('|','').strip())
            # print(shop_tel)
        except:
            pass
        shop_infor_list_p = []
        try:
            shop_infor_list = bs_content.find(class_='block-inner desc-list').find_all('dl')
            for shop_infor in shop_infor_list:
                shop_infor_list_p.append(shop_infor.get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('\xa0',''))
            for infor_item in shop_infor_list_p:
                if '营业时间' in infor_item:
                    shop_hours = infor_item.replace('修改','')
                elif '简介' in infor_item or '描述' in infor_item :
                    shop_intro = infor_item
                elif '别名' in infor_item:
                    shop_nick_name = infor_item
            # print(shop_hours)
            # print(shop_nick_name)
            # print(shop_intro)
        except:
            pass

        try:
            shop_comment_list = bs_content.find('div', attrs={'class': 'comment-rst'})
            # print(shop_comment_list)
        except:
            pass

        try:
            shop_rank = shop_comment_list.find('span', attrs={'class':'item-rank-rst irr-star40'})['title'].replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_rank)
        except:
            pass
        try:
            shop_comment_num = shop_comment_list.find('a',attrs = {'class':'count'}).get_text().replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_comment_num)
        except:
            pass

        try:
            shop_avg_price = shop_comment_list.find('dl').get_text().replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_avg_price)
        except:
            pass

    except:
        pass


    record_item = [dianping_url, shop_name, shop_city, shop_district_1, shop_district_2, shop_district_3, shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]
    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        # print(record_item)
        with open('dianping_page.csv', 'a', encoding='utf-16', newline='') as csvfile:
            fp_writer = csv.writer(csvfile, dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)
    else:
        pass


def webpages_hotel_get_details(bs_content):
    shop_name= ''
    shop_city= '',
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_district_4 = ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:

        try:

            shop_name = bs_content.find('div', attrs={'class':'body hotel-body'}).h1.get_text().replace('\n','').replace('|','').strip()
            # print(shop_name)
        except:
            pass


        try:
            shop_city = bs_content.find('a', attrs={'class':'city J-city'}).get_text().replace('\n','').replace('|','').strip().replace('站','')
            # print(shop_city)
        except:
            pass

        try:
            shop_district_1 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_1)
        except:
            pass
        try:
            shop_district_2 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_2)
        except:
            pass
        try:
            shop_district_3 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_3)
        except:
            pass
        try:
            shop_district_4 = bs_content.find('div',attrs= {'class':'breadcrumb'}).a.find_next('a').find_next('a').find_next('a').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_4)
        except:
            pass
        try:
            shop_address = bs_content.find('p',attrs={'class':'shop-address'}).get_text().replace('\n','').replace('|','').strip()
            # print(shop_address)
        except:
            pass
        try:
            shop_tel = str(bs_content.find('div',attrs={'class':'hotel-facilities'}).p.get_text().replace('\n','').replace('|','').strip().replace('&nbsp',''))
            # print(shop_tel)

        except:
            pass

        try:
            shop_intro = bs_content.find('p',attrs={'class':'J_hotel-description'}).get_text().replace('\n','').replace('|','').strip().replace('&nbsp','')
            # print(shop_intro)
        except:
            pass

        try:
            shop_comment_num = bs_content.find('div', attrs={'id': 'base-info'}).find('p', attrs = {'class':'info shop-star'}).find('span', attrs={'class':'item'}).get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('(','').replace(')','')+'条评论'
            # print(shop_comment_num)
        except:
            pass

    except:
        pass


    record_item = [dianping_url, shop_name, shop_city, shop_district_1, shop_district_2, shop_district_3, shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]
    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        with open('dianping_page.csv', 'a', encoding='utf-16', newline='') as csvfile:
            fp_writer = csv.writer(csvfile, dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)
    else:
        pass

def webpages_marriage_get_details(bs_content):
    shop_name= ''
    shop_city= '',
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_district_4 = ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:
        try:

            shop_name = bs_content.find('div', attrs={'class':'fl merchant-detail js-merchant-detail'}).h2.get_text().replace('\n','').replace('|','').strip()
            # print(shop_name)
        except:
            pass


        try:
            shop_city = bs_content.find('a', attrs={'class':'city J-city'}).get_text().replace('\n','').replace('|','').strip().replace('站','')
            # print(shop_city)
        except:
            pass

        try:
            shop_district_1 = bs_content.find('ul',attrs= {'class':'crumbs-list clearfix'}).li.get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_1)
        except:
            pass
        try:
            shop_district_2 = bs_content.find('ul',attrs= {'class':'crumbs-list clearfix'}).li.find_next('li').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_2)
        except:
            pass
        try:
            shop_district_3 = bs_content.find('ul',attrs= {'class':'crumbs-list clearfix'}).li.find_next('li').find_next('li').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_3)
        except:
            pass
        try:
            shop_district_4 = bs_content.find('ul',attrs= {'class':'crumbs-list clearfix'}).li.find_next('li').find_next('li').find_next('li').get_text().replace('\n','').replace('|','').strip()
            # print(shop_district_4)
        except:
            pass



        try:
            shop_address = bs_content.find('li',attrs={'class':'info-item clearfix'}).find('span').find_next('span').get_text().replace('\n','').replace('|','').strip()
            # print(shop_address)
        except:
            pass
        try:
            shop_tel = str(bs_content.find('span',attrs = {'class':'info-con hightlight'}).get_text().replace('\n','').replace('|','').strip())
            # print(shop_tel)
        except:
            pass
        shop_infor_list_p = []
        try:
            shop_infor_list = bs_content.find(class_='block-inner desc-list').find_all('dl')
            for shop_infor in shop_infor_list:
                shop_infor_list_p.append(shop_infor.get_text().replace('\n', '').replace('|', '').replace(' ', '').replace('\xa0',''))
            for infor_item in shop_infor_list_p:
                if '营业时间' in infor_item:
                    shop_hours = infor_item.replace('修改','')
                elif '简介' in infor_item or '描述' in infor_item :
                    shop_intro = infor_item
                elif '别名' in infor_item:
                    shop_nick_name = infor_item
            # print(shop_hours)
            # print(shop_nick_name)
            # print(shop_intro)
        except:
            pass

        # try:
        #     shop_comment_list = bs_content.find('div', attrs={'class': 'comment-rst'})
        #     # print(shop_comment_list)
        # except:
        #     pass

        try:
            shop_rank = bs_content.find('div', attrs={'class':'merchant-review mb15'}).find('span')['title'].replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_rank)
        except:
            pass
        try:
            shop_comment_num = bs_content.find('a',attrs = {'class':'review-number'}).get_text().replace('\n', '').replace('|', '').replace(' ', '')
            # print(shop_comment_num)
        except:
            pass

    except:
        pass


    record_item = [dianping_url, shop_name, shop_city, shop_district_1, shop_district_2, shop_district_3, shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]
    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        with open('dianping_page.csv', 'a', encoding='utf-16', newline='') as csvfile:
            fp_writer = csv.writer(csvfile, dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)
    else:
        pass


def webpages_decoration_get_details(bs_content):
    shop_name= ''
    shop_city= '',
    shop_district_1= ''
    shop_district_2= ''
    shop_district_3= ''
    shop_district_4 = ''
    shop_address= ''
    shop_tel= ''
    shop_hours= ''
    shop_comment_num= ''
    shop_avg_price= ''
    shop_rank= ''
    shop_rank_1= ''
    shop_rank_2= ''
    shop_rank_3= ''
    shop_rank_4= ''
    shop_lat = ''
    shop_lng = ''
    shop_intro = ''
    shop_nick_name = ''
    try:

        try:

            shop_name = bs_content.find('div', attrs={'class':'shop-name'}).h1.get_text().replace('\n','').replace('|','').strip()
            print(shop_name)
        except:
            pass


        try:
            shop_city = bs_content.find('a', attrs={'class':'city J-city'}).get_text().replace('\n','').replace('|','').strip().replace('站','')
            print(shop_city)
        except:
            pass

        try:
            shop_district_1 = bs_content.find('div',attrs= {'class':'breadcrumb-wrapper'}).ul.li.get_text().replace('\n','').replace('|','').strip()
            print(shop_district_1)
        except:
            pass
        try:
            shop_district_2 = bs_content.find('div',attrs= {'class':'breadcrumb-wrapper'}).ul.li.find_next('li').get_text().replace('\n','').replace('|','').strip()
            print(shop_district_2)
        except:
            pass
        try:
            shop_district_3 = bs_content.find('div',attrs= {'class':'breadcrumb-wrapper'}).ul.li.find_next('li').find_next('li').get_text().replace('\n','').replace('|','').strip()
            print(shop_district_3)
        except:
            pass
        try:
            shop_district_4 = bs_content.find('div',attrs= {'class':'breadcrumb-wrapper'}).ul.li.find_next('li').find_next('li').find_next('li').get_text().replace('\n','').replace('|','').strip()
            print(shop_district_4)
        except:
            pass



        try:
            shop_address = bs_content.find('p',attrs={'class':'shop-contact address'}).find('a',attrs = {'class':'region'}).next_element.replace('\n','').replace('|','').strip()
            print(shop_address)
        except:
            pass
        try:
            shop_tel = str(bs_content.find('div',attrs = {'class':'shop-contact telAndQQ'}).find('strong').get_text().replace('\n','').replace('|','').strip())
            print(shop_tel)
        except:
            pass
        try:
            shop_hours = bs_content.find('div',attrs={'class':'business-card clearfix'}).find('td').find_next('td').get_text().replace('\n','').replace('|','').strip()
            print(shop_hours)
            shop_intro = bs_content.find('div',attrs={'class':'business-card clearfix'}).find('p').get_text().replace('\n','').replace('|','').strip()
            print(shop_intro)

        except:
            pass

        # try:
        #     shop_comment_list = bs_content.find('div', attrs={'class': 'comment-rst'})
        #     # print(shop_comment_list)
        # except:
        #     pass

        try:
            shop_rank = bs_content.find('div', attrs={'class':'comment-rst'}).find('span')['title'].replace('\n', '').replace('|', '').replace(' ', '')
            print(shop_rank)
        except:
            pass
        try:
            shop_comment_num = bs_content.find('div', attrs={'class':'comment-rst'}).find('a').get_text().replace('\n', '').replace('|', '').replace(' ', '')
            print(shop_comment_num)
        except:
            pass

        try:
            shop_avg_price = bs_content.find('div', attrs={'class':'comment-rst'}).find('span',attrs = {'class':'avg-price'}).get_text().replace('\n', '').replace('|', '').replace(' ', '')
            print(shop_avg_price)
        except:
            pass

    except:
        pass


    record_item = [dianping_url, shop_name, shop_city, shop_district_1, shop_district_2, shop_district_3, shop_address,
              shop_tel,shop_hours,shop_comment_num,shop_avg_price,shop_rank,shop_rank_1,shop_rank_2,
             shop_rank_3,shop_rank_4,shop_intro,shop_nick_name, shop_lat,shop_lng]
    print(record_item)
    if shop_name != '' and shop_city != "('',)":
        with open('dianping_page.csv', 'a',
                  encoding='utf-16',
                  newline='') as csvfile:
            fp_writer = csv.writer(csvfile, dialect='excel', delimiter='|')
            fp_writer.writerow(record_item)
    else:
        pass

###53227634

root_url = 'http://www.dianping.com/shop/'
for i in range(0, 5000000):
    # dianping_url = 'http://www.dianping.com/shop/6200046'
    shop_address = None
    shut_down_notice = None
    shut_down_notice_1 = None
    shut_down_notice_2 = None
    address_try_1 = None
    address_try_2 = None
    address_try_3 = None
    address_try_4 = None
    address_try_5 = None
    address_try_6 = None
    dianping_url = root_url + str(i)
    print(dianping_url)
    try:
        resp = url_connect_retry(dianping_url)
        raw_content = resp.text
        bs_content = BeautifulSoup(raw_content, 'lxml')
        # print(bs_content)
    except Exception as e:
        print(dianping_url, e)
        # print(raw_content)
    # print('关店信息原来是 %s' % shut_down_notice)
    try:
        shut_down_notice = bs_content.find('div',attrs={'class':'msg msg-pause'}).find('strong').next_element

    except:
        pass
    # print('关店信息现在是 %s' % shut_down_notice)
    # print('1原来是%s'%shut_down_notice_1)
    try:
        shut_down_notice_1 = bs_content.find('p',attrs={'class':'shop-closed'})
    except:
        pass
    # print('1现在是%s' %shut_down_notice_1)
    # print('2原来是%s'%shut_down_notice_2)
    try:
        shut_down_notice_2 = bs_content.find('p',attrs={'class':'nobusiness-action shop-closed'})
    except:
        pass
    # print('2现在是%s' %shut_down_notice_2)


    try:
        address_try_1 = bs_content.find('div',attrs={'class':'expand-info address'}).find('span',attrs ={'class':'item'}).get_text().replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)
    try:
        address_try_2 = bs_content.find('div',attrs={'class':'shop-addr'}).find('span')['title'].replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)
    try:
        address_try_3 = bs_content.find('div',attrs={'class':'desc-list'}).find('dt').get_text() +bs_content.find('div',attrs={'class':'desc-list'}).find('span',attrs = {'itemprop':'street-address'}).get_text().replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)
    try:
        address_try_4 = bs_content.find('p',attrs={'class':'shop-address'}).get_text().replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)
    try:
        address_try_5 = bs_content.find('li',attrs={'class':'info-item clearfix'}).find('span').find_next('span').get_text().replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)
    try:
        address_try_6 = bs_content.find('p',attrs={'class':'shop-contact address'}).find('a',attrs = {'class':'region'}).next_element.replace('\n','').replace('|','').strip()
    except:
        pass
        # print(dianping_url, e)


    if shut_down_notice != None or shut_down_notice_1 != None or shut_down_notice_2 != None:
        print(dianping_url, '店铺关门或者暂停收录')
        continue
    if address_try_1 != None:
        print('普通页面类型')
        webpages_get_details(bs_content)
    elif address_try_2 != None:
        print('亲子页面类型')
        webpages_kids_get_details(bs_content)
    elif address_try_3!= None:
        print('家居页面类型')
        webpages_housing_get_details(bs_content)
    elif address_try_4!= None:
        print('酒店页面类型')
        webpages_hotel_get_details(bs_content)
    elif address_try_5!= None:
        print('婚宴页面类型')
        webpages_marriage_get_details(bs_content)
    elif address_try_6!= None:
        print('装修页面类型')
        webpages_decoration_get_details(bs_content)
    else:
        pass

