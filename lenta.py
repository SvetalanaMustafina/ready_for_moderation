# ssh -L 8089:46.4.52.153:5432 psqlreader@46.4.52.153
from sqlalchemy import create_engine
import pandas as pd
import time
import requests
import json
from urllib.parse import urlencode
from telegram_api import *
import numpy as np
import psycopg2
# gobs_proccessed = []
# waves = ['lm_zs_2021_21_KVI_LOCAL-27-05-13','lm_zs_2021_21_KVI_LONG-27-05-13','lm_zs_2021_21_KVI_SHORT-27-05-13'] #,'lm_zs_2021_21_KVI_SOF-27-05-11']
waves = ['lm_zs_2021_21_KVI_LOCAL-27-05-15','lm_zs_2021_21_KVI_LONG-27-05-15','lm_zs_2021_21_KVI_SHORT-27-05-15','lm_zs_2021_21_KVI_SOF-27-05-15']
db = create_engine('postgresql://psqlreader:aImf3fivls34@127.0.0.1:8089/ma_metro_forapp_mobi')
# db = psycopg2.connect(dbname='ma_metro_forapp_mobi', user='psqlreader',
#                         password='aImf3fivls34', host='localhost', port=8089)
path = '/Users/admin/Documents/git/ready_for_mod/tmp/'
# 1 - оповещать модераторов о прогруженных точках
# 2 - оповещать мдераторов о не прогруженных точках, кол-во прогруженных отчетов по 2 анкете, план по точкам
# 3 - оповещать модераторов о полностью прогруженных городах
# 4 - оповещать модераторов о статусе сборов в городах, кол-во прогруженных отчетов по 2 анкете во всем городе, план по городу
# 5 - 
# def send_message(message):
#     token = '1452857535:AAGyVZZQqYZfu92WOMO-L5BPqXVxT2BAoI0'
#     chat_id = 299487888  # your chat id

#     # message = 'test'  # text which you want to send

#     params = {'chat_id': chat_id, 'text': message}

#     base_url = f'https://api.telegram.org/bot{token}/'
#     url = base_url + 'sendMessage?' + urlencode(params)
#     # Only if you need it
#     # proxy = {'https': 'https://77.48.23.199:57842'}

#     # To send request via proxy
#     # resp = requests.get(url, proxies=proxy)
#     resp = requests.get(url)

def get_plan(wave):
# получить план по точкам
    merger = pd.DataFrame()
    for wave in waves:
        q = f"select t.id as task_id, gob.territory, gob.price_cluster, t.geo_object_id, gob.title, active_article_ids \
            from ma_metro.tasks t join ma_metro.geo_objects gob on gob.id=t.geo_object_id where t.wave in ('{wave}') and t.project_id = 1"
        df = pd.read_sql_query(q, db)
        df['wave'] = wave
        merger = merger.append(df, ignore_index = True)
    merger = merger.explode('active_article_ids')
    merger['active_article_ids'] = merger['active_article_ids'].astype('int64')
    merger['territory'] = merger.apply(lambda x: x['territory'].upper(), 1)
    # print(merger.info())
    # resg = merger.groupby(['wave','territory','geo_object_id']).agg({'active_article_ids':'nunique'}).reset_index()
    # print(resg)
    # exit()
    return merger

def get_fact(wave):
    # получить факт по точкам
    merger1 = pd.DataFrame()
    for wave in waves:
        q1 = f"select r.id as report_id, t.geo_object_id, r.article_id from  \
        ma_metro.reports r join ma_metro.tasks t on t.id=r.task_id where t.wave in ('{wave}') and t.project_id = 2"
        df1 = pd.read_sql_query(q1, db)
        df1['wave'] = wave
        merger1 = merger1.append(df1, ignore_index = True)
    
    q2 = f"select max(r.created_at) as last_create from ma_metro.reports r join ma_metro.tasks t on t.id=r.task_id \
         where t.wave in ('{wave}') and t.project_id = 2"
    p = pd.read_sql_query(q2, db)
    p = str(p.last_create.to_list())[12:-10]
    # df1['last_create'] = p
    # print(df1.last_create.loc[0])
    # print(merger1.head())
    resg = merger1.groupby(['wave','geo_object_id']).agg({'article_id':'nunique'}).reset_index()
    # print(resg)
    # exit()
   
    return merger1, p

def get_rate(df,df1):
    # найти долю выполнения
    res = df.merge(df1, how = 'left', left_on = ['wave','active_article_ids', 'geo_object_id'], right_on = ['wave','article_id', 'geo_object_id'])
    # print(res.head(100))
    resg = res.groupby(['wave','geo_object_id','territory','price_cluster','title']).agg({'active_article_ids':'nunique', 'article_id':'nunique'}).reset_index()
    # print(res[pd.isna(res['article_id'])])
    resg['rate'] =  resg['article_id'] / resg['active_article_ids']
    resg.sort_values(by='geo_object_id', inplace = True)
    # print('план\n',resg)
    print('непрогруженные точки:\n', resg[resg.rate != 1])
    resg.to_excel( path + 'top_tt.xlsx', index=False)
    # print('кол-во всего точек\n',resg.shape[0])
    # exit()
    return resg

def checked_points(resg):
    # найти выполненные точки
    check_geo_points = resg[resg['rate']==1]
    # print(check_geo_points)
    print('кол-во выполненных точек\n',check_geo_points.shape[0])
    # exit()
    return check_geo_points

def checked_cities(resg,city_proccessed):
    # найти выполненные города
    all_cities = resg.groupby(['wave','territory']).agg({'geo_object_id':'count', 'rate':'sum'}). \
        rename(columns = {'geo_object_id':'total_tt','rate':'total_rate'}).reset_index()
    all_cities['checked_city'] = np.where(all_cities["total_tt"] <= all_cities["total_rate"], True, False)
    print('all_cities\n',all_cities)
    checked_cities = all_cities[all_cities.checked_city == True]
    flag = False
    message='\u2757\ufe0f \u2757\ufe0f ГОТОВЫЕ ГОРОДА по 2 анкете:\n\n'
    for wave in waves:
        for city in checked_cities.territory.to_list():
            if city not in city_proccessed:
                message = message + f"{wave} - {city}\n"
                # message += message
                
                # send_message(message)
                flag = True
        # to do дописать логику, если нет ни одного прогруженного города

        # send_message(message)

        # if message == '':
        #     message = 'нет новых загруженных городов'
        #     send_message(message)
        
        # city_proccessed.extend(checked_cities.territory.to_list())

    if flag:
        send_message(message)
        print('IM HERE CITIES')
        city_proccessed.extend(checked_cities.territory.to_list())
        message2 = f'\u2757\ufe0f \u2757\ufe0f Остались непрогруженные города:\n{all_cities.loc[~all_cities.checked_city].drop(columns = ["checked_city"])}'
        # send_message(message2)
    # exit()
    return city_proccessed

def checked_wave(resg, wave_proccessed):
    # найти полностью готовые волны
    all_waves = resg.groupby(['wave']).agg({'territory':'count', 'rate':'sum'}). \
        rename(columns = {'territory':'total_cities','rate':'total_rate'}).reset_index()
    all_waves['checked_waves'] = np.where(all_waves["total_cities"] <= all_waves["total_rate"], True, False)
    # print('all_waves\n', all_waves[all_waves.checked_waves == False])
    print('all_waves\n', all_waves.sort_values(by='checked_waves'))
    checked_waves = all_waves[all_waves.checked_waves == True]
    # exit()
    flag = False
    message='\u270c\ufe0f ГОТОВЫЕ ВОЛНЫ по 2 анкете:\n\n'
    for wave in checked_waves.wave.to_list():
        if wave not in wave_proccessed:
            message = message + f"{wave}\n"
            flag = True

    # send_message(message)

    # if message == '':
    #     message = 'нет новых готовых волн'
    #     send_message(message)
    
    # wave_proccessed.extend(checked_waves.wave.to_list())

    # exit()
    if flag:
        send_message(message)
        wave_proccessed.extend(checked_waves.wave.to_list())
        print('IM HERE WAVE')
        message2 = f'\u270c\ufe0f Остались непрогруженные волны:\n{all_waves.loc[~all_waves.checked_waves].drop(columns = ["checked_waves"])}'
        # send_message(message2)
    return wave_proccessed



# найти точки, которые уже выгружены, но о которых мы еще не сообщали в модерацию
def new_checked_points(resg, gobs_proccessed):
    # gobs_proccessed = [] # сохраняет промежуточный список
    all_geo = resg.groupby(['wave','territory','geo_object_id']).agg({'rate':'sum'}). \
        rename(columns = {'rate':'total_rate'}).reset_index()
    all_geo['checked_geo'] = np.where(all_geo['total_rate']==1, True, False)
    # print('all_geo\n',all_geo)
    checked_geo = all_geo[all_geo.checked_geo == True]
    flag = False
    message =  f"ГОТОВЫЕ ТТ\n_____\n"
    # new_check_geo_points = check_geo_points[~check_geo_points['geo_object_id'].isin(gobs_proccessed)]
    # gobs_proccessed.extend(new_check_geo_points.geo_object_id.to_list())
    # while T
    

    for wave in waves:
        for gob in checked_geo.geo_object_id.to_list():
            if gob not in gobs_proccessed:
                # как вывести город для точки?
                # message = message + f"{wave} - {checked_geo.territory} - {gob}\n"
                message = message + f"{wave} - {gob}\n"
                flag = True
                # messages.append(message)

    # send_message(message)

        # if message == '':
        #     message = 'нет новых загруженных точек'
        #     send_message(message)
    
    # gobs_proccessed.extend(checked_geo.geo_object_id.to_list())

    # подготовить сообщение в модерацию об этих точках
    
    # for gob in new_check_geo_points.geo_object_id.to_list():
        # message = f"внимание! есть 100 отчетов по второй анкете для волны {wave} и точки {gob}"
        # send_message(message)
        # messages.append(message)
    # print(messages)
    # print(new_check_geo_points)
    # print('уже обработали', gobs_proccessed)
    if flag:
        # message1 = f"ГОТОВЫЕ ТТ\n_____\n"
        send_message(message)
        print('IM HERE GEO')
        gobs_proccessed.extend(checked_geo.geo_object_id.to_list())
        message2 = f'\u2757\ufe0f \u2757\ufe0f Остались непрогруженные точки:\n{all_geo.loc[~all_geo.checked_geo].drop(columns = ["checked_geo"])}'
        send_message(message2)
        # msg = f'\u2757\ufe0f \u2757\ufe0f Остались непрогруженные точки:\n{all_geo.loc[~all_geo.checked_geo].drop(columns = ["checked_geo"])}'
        # send_message(msg)
    return gobs_proccessed

def ostatok(df, df1, p):
    l = df.shape[0] - df1.shape[0]
    message = f'Осталось незагруженных точек: {l} из {df.shape[0]}\nВремя последней загрузки задания: {p}'
    send_message(message)
    # msg = f'Время последней загрузки задания: {p}'
    # send_message(msg)

def main():
    df = get_plan(waves)
    gobs_proccessed = [] # сохраняет промежуточный список
    city_proccessed = []
    wave_proccessed = []
    m = 0
    while True:
        df1, p = get_fact(waves)
        df_rate = get_rate(df,df1)
        # check_geo_points = checked_points(df_rate, gobs_proccessed)
        # gobs_proccessed = new_checked_points(df_rate, gobs_proccessed)
        city_proccessed = checked_cities(df_rate,city_proccessed)
        wave_proccessed = checked_wave(df_rate, wave_proccessed)
        print(f'Im here {m}')
        m += 1
        # exit()
        time.sleep(60)
        # exit()
        # ostatok(df_rate, check_geo_points, p)
        # time.sleep(120)

main()

