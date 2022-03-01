# coding:utf-8



from __future__ import print_function
import requests


apiKey = ''
url = "http://api01.idataapi.cn:8000/sight/ctrip?apikey=<azrfbky1je5qAQcAK6EU0SkokqT5cHhOKqLdVewsdrf3tIrgpAMrHOZsu2EW1Cm3>&cityid=26&sort=1"
headers = {
    "Accept-Encoding": "gzip",
    "Connection": "close"
    }


if __name__ == "__main__":
    r = requests.get(url, headers=headers)
    json_obj = r.json()
    print(json_obj)