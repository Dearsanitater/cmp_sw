
import os
#os.chdir('..')
import opg
import sys
from django.shortcuts import render
import json
from django.http import JsonResponse
import compare
import ergodic
json_dir=r'resource/test_report/'
def comp (request):

    src = 'db2';tgt = 'oboracle';err_handling = 1;if_cpdata=1#mysql##/oracle#1：自动忽略所有无主键，2：忽略所有出错表，3：忽略所有问题表，4：手动选择
    p=compare.ergodic_database()
    #print(compare.config)
    dump_excel = ergodic.get_casefile()
    p.compare(src, tgt, err_handling,if_cpdata)
    with open(f'{json_dir}err.txt','r+') as f:
        err_data=f.read()
    return render(request, 'json.html', {'data': err_data})
    #p.dump_e(dump_excel)**

def ajaxGetTxt(request):
    with open(f'{json_dir}cons.txt','r+') as f:
        cons=json.load(f)
    with open(f'{json_dir}report.txt','r+') as f:
        report=json.load(f)
    if request.is_ajax():
        return JsonResponse({'cons约束对比':cons,'report内容对比报告':report},safe=False)
    else:return JsonResponse({'cons约束对比':cons,'report内容对比报告':report},safe=False)