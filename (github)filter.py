# -*- coding: utf-8 -*-
from py2neo import Graph
import xlrd

import re
import time
import pandas as pd

from xlutils.copy import copy

time_start = time.time()
# --------------------------------------------

graph = Graph('http://localhost:7474', auth=("neo4j", "s65923290"))
print("连接成功！")
filename = 'data.xls'

rb = xlrd.open_workbook(filename, formatting_info=True)

wb = copy(rb)

ws = wb.get_sheet(1)

data = xlrd.open_workbook(filename, encoding_override='utf-8')

table = data.sheets()[0]

nrows = table.nrows
ncols = table.ncols

allnrows = 0
allwords = 0
dis = 0
sign = 0
lel = 0
updown = 0
exact = 0
exactly = 0
anexact = 0
no = 0
no1 = 0
entity1 = 0
topo = 0
lel1 = 0

# start
for i in range(1, nrows):
    print("abstract number", i)

    allnrows += 1

    alldata = table.row_values(i)

    getdata_1 = alldata[2]

    list_1 = getdata_1.split(", ")

    lsunreatched = []

    lsuntreated = []

    lstreated = []

    lsresult = []

    lsuntreat = []

    alldata_kk = table.row_values(i - 1)

    getdata_kk = alldata_kk[2]

    list_kk = getdata_kk.split(", ")

    # exact matching
    for j in list_1:
        allwords += 1

        data_1 = graph.run(
            "match(n:Province)-[:`affiliation`]->(m:Province) where n.name= '" + j + "'return m.name").data()
        if len(data_1) == 0:
            data_2 = graph.run(
                "match(n:Province)-[:`affiliation`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()

            if len(data_2) == 0:
                lsunreatched.append(j)

            elif len(data_2) == 1:

                result_1 = j + "(" + data_2[0]["n.name"] + ")" + "->" + data_2[0]["m.name"]
                lsresult.append(result_1)
                lstreated.append(j)
                anexact += 1
            else:
                lsuntreated.append(j)

        elif len(data_1) == 1:
            result_1 = j + "->" + data_1[0]["m.name"]
            lsresult.append(result_1)
            lstreated.append(j)
            exact += 1
            exactly += 1
        else:
            lsuntreated.append(j)

            # word spacing
    for j in lsuntreated:
        data_2 = graph.run(
            "match(n:Province)-[:`affiliation`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()
        ls_temp = []
        for m in data_2:

            if m['n.name'] in lstreated:
                ls_temp.append(m)

        if len(ls_temp) == 1:
            result_2 = j + "(" + ls_temp[0]["n.name"] + ")" + "->" + ls_temp[0]["m.name"]
            lsresult.append(result_2)
            lsuntreated.remove(j)
            lstreated.append(j)
            sign += 1

    for j in lsuntreated:

        data_dis1 = graph.run(
            "match(n:Province)-[:`affiliation`]->(m:Province) where n.name= '" + j + "' or n.anothername= '" + j + "'return n.name, m.name, m.anothername,n.pac,m.level,n.level").data()
        data_dis2 = graph.run(
            "match(n:Province)<-[:`affiliation`]-(m:Province) where n.name= '" + j + "' or n.anothername= '" + j + "'return n.name, m.name, m.anothername,n.pac,m.level,n.level").data()

        data_dis = data_dis1 + data_dis2
        ls_temp = []
        for m in data_dis:

            if m["m.name"] in list_1 and m["m.name"] != j:

                ls_temp.append(m)

            elif m["m.anothername"] in list_1 and m["m.anothername"] != j:
                ls_temp.append(m)
        if len(ls_temp) == 1:

            nname_pac = str(ls_temp[0]["n.pac"])

            data_name = graph.run(
                "match(n:Province)-[:`affiliation`]->(m:Province) where n.pac= '" + nname_pac +
                "'return m.name, n.name").data()

            if j == data_name[0]["n.name"]:

                result_3 = j + "->" + data_name[0]["m.name"]
            else:

                result_3 = j + "(" + data_name[0]["n.name"] + ")" + "->" + data_name[0]["m.name"]
            lsresult.append(result_3)
            lsuntreated.remove(j)
            lstreated.append(j)
            updown += 1

        elif len(ls_temp) > 1:
            getdata_2 = alldata[2]
            weizhi = []
            for g in range(len(ls_temp)):
                one_gjz = []

                for d in re.finditer(ls_temp[g]["m.anothername"], getdata_2):

                    t = d.span()[0]

                    one_gjz.append(ls_temp[g]["m.anothername"] + ' ' + str(t))

                weizhi.append(one_gjz)

                for d1 in re.finditer(j, getdata_2):

                    t = d1.span()[0]

                    zdgjz = []
                    lszdgjz = []

                    zdgjz.append(j + ' ' + str(t))

                lszdgjz.append(zdgjz)

            zuixinao_true = []
            zuixiao_zhi = []

            for e in range(len(weizhi)):
                zuixinao = []

                for h in range(len(weizhi[e])):

                    for z in range(len(zdgjz)):

                        zd_all = zdgjz[z].split(' ')

                        qt_all = weizhi[e][h].split(' ')

                        jieli = abs(int(zd_all[1]) - int(qt_all[1]))

                        zuixinao.append(jieli)

                zdgjzh = zdgjz[0].split(' ')[0]

                qtgzdh = weizhi[e][0].split(' ')[0]

                dict1 = {
                    f'{qtgzdh}': min(zuixinao)
                }

                zuixinao_true.append(dict1)

                zuixiao_zhi.append(min(zuixinao))

            minkey = list(zuixinao_true[zuixiao_zhi.index(min(zuixiao_zhi))])

            minvalue = min(zuixiao_zhi)
            lsanothername = []
            lspac = []
            for mm in ls_temp:

                lsanothername.append(mm["m.anothername"])

                lspac.append(mm["n.pac"])

            if minvalue <= 10:

                nname_pac = str(lspac[lsanothername.index(minkey[0])])

                data_disname = graph.run(
                    "match(n:Province)-[:`affiliation`]->(m:Province) where n.pac= '" + nname_pac + "' return m.name, n.name").data()

                if j == data_disname[0]["n.name"]:

                    result_4 = j + "->" + data_disname[0]["m.name"]
                else:

                    result_4 = j + "(" + data_disname[0]["n.name"] + ")" + "->" + data_disname[0]["m.name"]
                lsresult.append(result_4)
                lsuntreated.remove(j)
                lstreated.append(j)
                dis += 1

            else:
                lslevel = []
                lsnpac = []

                for mm in ls_temp:

                    lslevel.append(mm["n.level"])

                    lsnpac.append(mm["n.pac"])

                minlev_acount = lslevel.count(min(lslevel))

                if minlev_acount == 1:

                    nname_pac = str(lsnpac[lslevel.index(min(lslevel))])

                    data_disname = graph.run(
                        "match(n:Province)-[:`affiliation`]->(m:Province) where n.pac= " + nname_pac + " return m.name, n.name").data()

                    if data_disname[0]["n.name"] == j:

                        result_5 = j + "->" + data_disname[0]["m.name"]
                    else:

                        result_5 = j + "(" + data_disname[0]["n.name"] + ")" + "->" + data_disname[0]["m.name"]
                    lsresult.append(result_5)
                    lsuntreated.remove(j)
                    lstreated.append(j)
                    lel += 1

        # identification
        for j in lsuntreated:

            data_2 = graph.run(
                "match(n:Province)-[:`affiliation`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()
            ls_temp = []
            for m in data_2:

                if m['n.name'] in lstreated:
                    ls_temp.append(m)

            if len(ls_temp) == 1:

                result_2 = j + "(" + ls_temp[0]["n.name"] + ")" + "->" + ls_temp[0][
                    "m.name"]
                lsresult.append(result_2)
                lsuntreated.remove(j)
                lstreated.append(j)
                sign += 1

    # scaling
    for j in lsuntreated:

        data_level2 = graph.run(
            "match(n:Province)-[:`affiliation`]->(m:Province) where n.anothername= '" + j + "' or n.name= '" + j + "'return n.name, m.name, m.level").data()
        lslevel_nname = []
        lslevel_mname = []
        lslevel_mlevel = []
        for m in data_level2:
            lslevel_nname.append(m["n.name"])
            lslevel_mname.append(m["m.name"])
            lslevel_mlevel.append(m["m.level"])

        minlevel_count = lslevel_mlevel.count(min(lslevel_mlevel))
        if minlevel_count == 1:
            min_level = min(lslevel_mlevel)

            max_nname = lslevel_nname[lslevel_mlevel.index(min_level)]

            max_mname = lslevel_mname[lslevel_mlevel.index(min_level)]

            if j == max_nname:
                result_5 = j + "->" + max_mname
            else:
                result_5 = j + "(" + max_nname + ")" + "->" + max_mname
            lsresult.append(result_5)
            lsuntreated.remove(j)
            lstreated.append(j)
            lel1 += 1
    dim = []
    for j in lsuntreated:

        data_3 = graph.run(
            "match(n:Province)-[:`adjacency`]->(m:Province) where n.name=~'" + j + ".*'return n.name,n.anothername, m.name,m.level,n.level").data()

        if len(data_3) > 0:
            ls_dim1 = []
            ls_dim2 = []
            ls_dimlevel = []

            for m in data_3:

                if str(m["n.level"]) < str(5):

                    ls_dim1.append(m)

            for m7 in ls_dim1:

                if m["n.name"] in lstreated or m["n.anothername"] in lstreated:

                    ls_dim2.append(m)

                    ls_dimlevel.append(m["m.level"])

            if len(ls_dim1) == 1:

                result = j + "(" + ls_dim1[0]["n.name"] + ")" + "->" + ls_dim1[0]["m.name"]
                lsresult.append(result)
                lsuntreated.remove(j)
                lstreated.append(j)

                dim.append(j)
                anexact += 1

                print("精确匹配消岐", j)

            elif len(ls_dim2) == 1:

                result = j + "(" + ls_dim1[0]["n.name"] + ")" + "->" + ls_dim1[0]["m.name"]
                lsresult.append(result)
                lsuntreated.remove(j)
                lstreated.append(j)
                dim.append(j)
                sign += 1
                print("标识法消岐", j)

            elif len(ls_dim2) > 1:

                level_count = ls_dimlevel.count(min(ls_dimlevel))
                if level_count == 1:

                    print(pd.DataFrame(ls_dim2))
                    result = j + "(" + ls_dim2.index(min(ls_dimlevel))["n.name"] + ")" + "->" + \
                             ls_dim2.index(min(ls_dimlevel))["m.name"]
                    dim.append(j)
                    lel += 1

                    print("尺度消岐", j)
                    lsresult.append(result)
                    lsuntreated.remove(j)
                    lstreated.append(j)
    no1 = no1 + len(lsuntreated)
    no = no + len(lsunreatched)
    lsuntreat = lsuntreated + lsunreatched


print("exact matching：", anexact)
print('word spacing：', updown)
print("identification：", sign + dis)
print("identification_scaling：", lel)
print('scaling：', lel1)

wb.save(filename)

time_end = time.time()
time_sum = time_end - time_start
print(time_sum)
