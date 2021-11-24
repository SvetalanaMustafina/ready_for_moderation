import requests
import json
from urllib.parse import urlencode
import time

def send_message(message):
    token = '1452857535:AAGyVZZQqYZfu92WOMO-L5BPqXVxT2BAoI0'
    chat_id = [299487888]  # your chat id
    # chat_id = 299487888
    # 209814679 - Стас
    # 264439086 - Саша
    # message = 'test'  # text which you want to send
    for i in chat_id:
        params = {'chat_id': i, 'text': message.encode('utf-8', 'strict')}
    # params = {'chat_id': chat_id, 'text': message}

        base_url = f'https://api.telegram.org/bot{token}/'
        url = base_url + 'sendMessage?' + urlencode(params)
    # Only if you need it
    # proxy = {'https': 'https://77.48.23.199:57842'}

    # To send request via proxy
    # resp = requests.get(url, proxies=proxy)
        resp = requests.get(url)
