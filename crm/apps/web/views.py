from django.shortcuts import render, HttpResponse
import sqlite3
import os
import pandas as pd
import configparser

def home(request):
    return HttpResponse("成功")
def report(request):
    #print(os.getcwd())
    # html_table=pd.read_excel('resource/test_report/report.xlsx',-1).to_html(classes="table table-striped",index=False)

    html_table = pd.read_excel('resource/mapping.xlsx','mapping_mysql').to_html(classes="table table-striped")
    #print(os.getcwd())
    return render(request,r'report.html',{'table':html_table})
def news(request, nid):
    print(nid)
    page = request.GET.get("page")
    return HttpResponse("新闻")


def config(request):
    config=configparser.ConfigParser()
    print(os.getcwd())
    config_path = os.path.abspath('resource/DB.ini')
    config.read(config_path,encoding='utf-8')
    config_data={};filt_data=[]
    for sec in config.sections():
        if sec!='400_type':
            config_data[sec] = dict(config.items(sec))
            if 'type' in config_data[sec]:
                filt_data.append(config_data[sec]['type'])
    filt_data=set(filt_data)
    print(filt_data)
    print(type(config_data),config_data)
    # conn = sqlite3.connect('identifier.sqlite')  # 连接到数据库
    # cursor = conn.cursor()
    # cursor.execute(r'select * from main.config')
    # a=cursor.fetchall()
    return render(request,r'DB.html',{'config_data':config_data,'filt_data':filt_data})