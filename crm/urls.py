"""crm URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
import os
from django.contrib import admin
from django.urls import path
#from apps.web import views
from crm.apps.web import views
from crm.apps.api import views as vapi
from crm.apps.api import apps
print(f'{os.getcwd()}')
urlpatterns = [
    path('admin/', admin.site.urls),
    path('home/', views.home),
    path('news/<int:nid>/edit',views.news),
    path('config/', views.config),
    path('report/', views.report),
    path('compare/',vapi.comp),
    path('rule/',apps.get_rule),
    path('rule/test/',apps.get_rule_test),
    path('rule/test/<str:rule_id>/',apps.get_rule_test),
    path('logs/',apps.django_logs),
    path('jsonrule/',apps.jsonrule),
    path('rule/create_rule/',apps.create_rule),
    path('rule/del_rule/<str:rule_id>/',apps.del_rule),
    path('rule/ch_rule/<str:rule_id>/',apps.ch_rule),

    #异步进程池
    path('rule/exec/<str:rule_id>/',apps.exec_switch),
    path('rule/comp/<str:rule_id>/',apps.exec_compare),
    path('rule/shut/<str:rule_id>/',apps.shut_rule),
    path('rule/pause/<str:rule_id>/',apps.pause_rule),
    path('rule/resume/<str:rule_id>/',apps.resume_rule),
    path('rule/kill/<str:rule_id>/',apps.kill_rule),
    path('rule/status/<str:rule_id>/',apps.task_status),
    path('get/data',vapi.ajaxGetTxt),
    #DB资源界面相关操作
    path('config/del/<str:dbname>',apps.del_config),
    path('config/update/<str:dbname>',apps.udp_config),
    path('config/add_config',apps.add_config),
    #detail/report界面
    path('rule/detail/<str:rule_id>/',apps.detail),
    path('media/excel/',apps.serve_excel),
    path('detail/downloadf',apps.download_ffile),
    path('detail/download',apps.serve_excel),
    #hbase数据查询界面
    path('select/hbase', apps.hbase_browser),
    path('select/hbase/sources', apps.hbase_sources),
    path('select/hbase/connect', apps.hbase_connect),
    path('select/hbase/tree', apps.hbase_tree),
    path('select/hbase/scan', apps.hbase_scan),
]
