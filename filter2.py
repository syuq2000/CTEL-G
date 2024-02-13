# -*- coding: utf-8 -*-
from py2neo import Graph, Node, Relationship
import xlrd
import xlwt
import re
import time
import pandas as pd
import jieba as jie
from xlutils.copy import copy
import aaa

time_start = time.time()  # 记录开始时间
# --------------------------------------------
# 连接neo4j数据库，输入地址、用户名、密码
graph = Graph('http://localhost:7474', auth=("neo4j", "s65923290"))
print("连接成功！")
filename = 'test1249副本-新.xls'  # 文件名（包含标题和摘要）
# 使用xlrd库打开一个Excel工作簿。open_workbook函数用于打开由filename变量指定的工作簿，
# formatting_info=True表示在读取工作簿时包含格式信息（必须设置，否则会抛出异常）
rb = xlrd.open_workbook(filename, formatting_info=True)

# 使用copy函数从一个已打开的Excel对象rb中创建一个副本，并将副本存储在wb变量中。
# 通过创建副本，可以对工作簿进行修改而不影响原始工作簿，这对于在同一个工作簿上进行多个操作或保存多个版本非常有用。
wb = copy(rb)  # 复制页面

# 获取副本工作簿wb的第二个sheet，并将其存储在ws变量中。get_sheet(1)函数用于获取指定索引的sheet，索引从0开始。
# 因此，表示获取第二个sheet（索引为1）。获取sheet后，可以对sheet进行读取或写入数据。
ws = wb.get_sheet(1)  # 取第二个sheet（为空白sheet）

# 文件只包含摘要，encoding_override属性只针对老版本excel，对新版本不生效
data = xlrd.open_workbook('test1249副本-新.xls', encoding_override='utf-8')

# 从副本中获取第一个sheet，并将其存储在table变量中。
# data.sheets()函数返回一个包含所有sheet的列表，通过索引[0]获取第一个sheet
table = data.sheets()[0]  # 选定表中得第一个sheet
# 获取table对象的行数，将结果存储在nrows变量中。nrows用于确定表格中数据的总行数。
nrows = table.nrows  # 获取表格行数
ncols = table.ncols  # 获取表格列数

#
diceneity = aaa.read_eneity()

allnrows = 0
allwords = 0
dis = 0
sign = 0
lel = 0
updown = 0  # 上下级
exact = 0
exactly = 0
anexact = 0  # 地名消歧准确结果
no = 0
no1 = 0
exactnrows = 0  # 没有歧义地名的摘要篇数
placename_cluster = 0  # 将具有相同待消岐行政区划的摘要进行合并消岐
entity1 = 0
topo = 0
lel1=0

# 开始消岐
for i in range(1, nrows):  # （副本文件）第0行为表头，从1开始即跳过表头
    print("处理摘要数：", i)
    # 记录处理的摘要篇数，每处理一行数据就+1
    allnrows += 1
    # 存储当前行的所有数据
    alldata = table.row_values(i)  # 循环输出excel表中每一行，即所有数据
    # 获取当前行的第三列（索引为2）
    getdata_1 = alldata[2]  # 取出表中第三列数据（待处理地名）
    # 将获取的列数据按“,”分隔成一个地名列表
    list_1 = getdata_1.split(", ")  # 将获取的列用“,”分开
    # 存储查询不到的地名
    lsunreatched = []  # 定义查不到的列表
    # 存储未处理的地名
    lsuntreated = []  # 定义未处理列表
    # 存储已处理的地名
    lstreated = []  # 定义已处理列表
    # 存储消岐结果
    lsresult = []  # 定义结果列表
    # lsenresult=[]#定义实体结果列表
    lsuntreat = []  # 定义行政区划无法消歧的地名
    # 存储前一行的所有数据
    alldata_kk = table.row_values(i - 1)
    # 获取前一行的第三列数据
    getdata_kk = alldata_kk[2]  # 第一个sheet第三列
    # 将获取的列数据按“,”分隔成一个地名列表
    list_kk = getdata_kk.split(", ")  # 将获取的列用“，”分开
    # 如果两个集合中的元素相同，代表该篇摘要不需要进行行政区划消岐，
    # 也就是将具有相同待消岐地名的摘要进行合并消岐，则将placename_cluster+1
    if set(list_1) == set(list_kk):
        placename_cluster += 1
    if alldata[3] == '':
        exactnrows += 1
    # if exactly == len(list_1):
    #     exactnrows += 1

    # 精确定位
    for j in list_1:  # 利用for循环迭代list_1中的每个元素
        allwords += 1
        # Cypher返回该地名的上级行政区划全称
        data_1 = graph.run(
            "match(n:Province)-[:`隶属`]->(m:Province) where n.name= '" + j + "'return m.name").data()
        # 全称查询不到进行简称查询
        if len(data_1) == 0:
            # Cypher返回该地名全称和上级行政区划全称
            data_2 = graph.run(
                "match(n:Province)-[:`隶属`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()
            # anothername是简称，也就是不带省市县的行政区划地名
            # 简称查询不到
            if len(data_2) == 0:
                lsunreatched.append(j)  # 加入查不到列表
            # 简称精确查询到只有一个对应的上级实体
            elif len(data_2) == 1:
                # 结果为j简称(j全称)->上级实体
                result_1 = j + "(" + data_2[0]["n.name"] + ")" + "->" + data_2[0]["m.name"]  # 查询有结果，j（简称） n（全称）隶属于m（全称）
                lsresult.append(result_1)  # 结果列表
                lstreated.append(j)  # 已处理列表
                anexact += 1  # 记录已消歧地名
            else:
                lsuntreated.append(j)  # （未处理）进行下一步——同名标识
                # print(lsuntreated)
        # 全称精确查询到只有一个对应的上级实体
        elif len(data_1) == 1:
            result_1 = j + "->" + data_1[0]["m.name"]  # 查询有结果，j->m
            lsresult.append(result_1)  # 结果列表
            lstreated.append(j)  # 已处理列表
            exact += 1  # 准确（无需消岐）的数量
            exactly += 1  # 摘要中无歧义地名数量
        else:
            lsuntreated.append(j)  # （未处理）进行下一步——同名标识

    # 同名标识法，这才是真正的词间距法（从已处理，也就是已标记实体出发，作为消岐依据）
    for j in lsuntreated:
        # Cypher查询该地名的简称，返回该地名的全称和上级行政区划全称
        data_2 = graph.run(
            "match(n:Province)-[:`隶属`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()
        ls_temp = []  # 标记实体的存储列表
        for m in data_2:
            # 如果全称n在已处理列表中，就将n加入标记实体列表
            if m['n.name'] in lstreated:
                ls_temp.append(m)
        # 如果标记实体的列表长度为1
        if len(ls_temp) == 1:
            # 结果为j简称（j全称）->上级实体
            result_2 = j + "(" + ls_temp[0]["n.name"] + ")" + "->" + ls_temp[0]["m.name"]  # 标识有结果，j(简称) n(全称)隶属于m(全称)
            lsresult.append(result_2)  # 加入结果列表
            lsuntreated.remove(j)  # 从未处理中删除j
            lstreated.append(j)  # 将j加入到已处理列表
            sign += 1  # 同名标识法数量

    # 词间距法消歧（从一篇文本内的原始地名出发，作为消岐依据）
    for j in lsuntreated:  # （未处理列表）
        # Cypher查询该地名的简称或全称，并返回包含该地名的全称、编码和等级，和上级行政区划的名称、简称、等级
        data_dis1 = graph.run(
            "match(n:Province)-[:`隶属`]->(m:Province) where n.name= '" + j + "' or n.anothername= '" + j + "'return n.name, m.name, m.anothername,n.pac,m.level,n.level").data()
        # Cypher查询该地名的简称或全称，并返回包含该地名的全称、编码和等级，和下级行政区划的名称、简称、等级
        data_dis2 = graph.run(
            "match(n:Province)<-[:`隶属`]-(m:Province) where n.name= '" + j + "' or n.anothername= '" + j + "'return n.name, m.name, m.anothername,n.pac,m.level,n.level").data()
        # 将包含该地名及其上下级行政区划的各种信息整合成一个列表
        data_dis = data_dis1 + data_dis2
        ls_temp = []  # 空列表，存储标记实体
        for m in data_dis:  # 对于每个在列表里的地名like或行政区划
            # 如果该地名的上（下）级行政区划全称在原始的待处理地名列表中
            # 且该地名的上（下）级行政区划全称不等于该地名
            # （PS:该地名的上或下级行政区划在图谱中！）
            if m["m.name"] in list_1 and m["m.name"] != j:  # list1为文本中待处理地名
                # 将该地名的上（下）级行政区划全称加入标记实体列表中
                ls_temp.append(m)
            # 否则如果该地名的上（下）级行政区划的简称在原始的待处理地名列表中
            # 且该地名的上（下）级行政区划简称不等于该地名
            # （PS：同上）
            elif m["m.anothername"] in list_1 and m["m.anothername"] != j:
                ls_temp.append(m)  # 把该地名的上（下）级行政区划简称加入标记实体列表中
        # 如果标记实体列表的长度为1（无歧义词间距法，在论文中叫做词间距法）
        if len(ls_temp) == 1:
            # 将这个唯一的元素中的键n.pac对应的值传递给nname_pac
            nname_pac = str(ls_temp[0]["n.pac"])
            # Cypher语句查询该编码的值，并返回该编码对应的行政区划全称以及上级行政区划全称
            data_name = graph.run(
                "match(n:Province)-[:`隶属`]->(m:Province) where n.pac= '" + nname_pac +
                "'return m.name, n.name").data()
            # 如果该地名与data_name中第一个元素（也是唯一一个元素）的n.name值相同
            if j == data_name[0]["n.name"]:  # 获取data_name中第一个元素的name值
                # 可以得到该地名j->m全称
                result_3 = j + "->" + data_name[0]["m.name"]  # n属于m
            else:
                # 否则，该地名j(n全称）->m全称
                result_3 = j + "(" + data_name[0]["n.name"] + ")" + "->" + data_name[0]["m.name"]  # j+n属于m
            lsresult.append(result_3)  # 加入结果列表
            lsuntreated.remove(j)  # 从未处理列表中移除j
            lstreated.append(j)  # 向已处理列表中添加j
            updown += 1  # 词间距法计数



        elif len(ls_temp) > 1:  # 有歧义的词间距，在论文中叫做标识法（通过计算j地名和标记实体列表中上级行政区划简称 之间的距离，取最小值为消岐结果）
            getdata_2 = alldata[2]  # 取出表中第三列数据（待处理地名）
            weizhi = []  # 定义空列表，存储关键字位置信息
            for g in range(len(ls_temp)):  # 遍历标记实体列表的长度范围
                one_gjz = []  # 定义空列表，存储一个关键字的位置信息
                # 通过正则表达式在待处理地名中查找标记实体列表中的上级行政区划简称
                for d in re.finditer(ls_temp[g]["m.anothername"], getdata_2):
                    # 将正则表达式匹配结果的起始位置赋值给t。
                    # span()返回一个元组，包含匹配结果的起始位置和结束位置，索引为0的元素表示匹配结果的起始位置
                    t = d.span()[0]
                    # print(guan[g],t)
                    # 将标记实体的上级行政区划简称（关键字）和起始位置信息添加到one_gjz列表中
                    one_gjz.append(ls_temp[g]["m.anothername"] + ' ' + str(t))
                # 将one_gjz列表添加到weizhi列表中，表示一个关键字的位置
                weizhi.append(one_gjz)
                # 通过正则表达式查找在待处理地名中查找还未处理的j地名
                for d1 in re.finditer(j, getdata_2):
                    # 获取j地名的起始位置
                    t = d1.span()[0]
                    # print(guan[g],t)
                    zdgjz = []  # 定义空列表，存储还未处理的j地名的位置信息
                    lszdgjz = []  # 定义空列表（是上个列表的统计集合）
                    # 将j地名和位置信息添加到列表中
                    zdgjz.append(j + ' ' + str(t))
                # 将j地名和位置信息组成的列表添加到定义的大列表中
                lszdgjz.append(zdgjz)
                # print('当前在原文中指定关键字（j地名）的位置:', lszdgjz)
                # print('当前在原文中所有关键字（标记实体列表中的上级行政区划简称）的位置:', weizhi)
            zuixinao_true = []  # 存储所有距离的相关信息（字典）的列表
            zuixiao_zhi = []  # 存储最小距离的数值
            # 遍历位置列表的长度范围
            for e in range(len(weizhi)):
                zuixinao = []  # 空列表，存储每个j地名减去标记实体的上级行政区划简称 的距离信息
                # 遍历每个标记实体的上级行政区划简称的位置信息
                for h in range(len(weizhi[e])):
                    # 遍历每个j地名的位置信息
                    for z in range(len(zdgjz)):
                        # 将j地名的位置信息拆分为j地名和位置值
                        zd_all = zdgjz[z].split(' ')
                        # 将标记实体上级行政区划简称位置信息拆分为简称和位置值
                        qt_all = weizhi[e][h].split(' ')
                        # 计算j地名和上级行政区划简称之间的距离（地名 -减 简称，取绝对值）
                        jieli = abs(int(zd_all[1]) - int(qt_all[1]))
                        # print(zd_all[0], '---', qt_all[0] + '\t', jieli)
                        # 将距离值添加到zuixinao列表中
                        zuixinao.append(jieli)
                # 获取j地名的名称（关键字）部分
                zdgjzh = zdgjz[0].split(' ')[0]
                # 获取标记实体上级行政区划简称（关键字）部分
                qtgzdh = weizhi[e][0].split(' ')[0]
                # 创建字典，关键字为标记实体上级行政区划简称，值为最小距离
                dict1 = {
                    f'{qtgzdh}': min(zuixinao)
                }
                # 将字典添加到zuixinao_true中
                zuixinao_true.append(dict1)
                # 将zuixinao列表中的最小距离添加到zuixiao_zhi中
                zuixiao_zhi.append(min(zuixinao))
            # 获取最小距离所对应的标记实体上级行政区划简称
            minkey = list(zuixinao_true[zuixiao_zhi.index(min(zuixiao_zhi))])
            # 获取最小距离值
            minvalue = min(zuixiao_zhi)
            lsanothername = []  # 存储标记实体的上级行政区划简称
            lspac = []  # 存储标记实体的编码
            for mm in ls_temp:  # 遍历标记实体列表
                # 将每个标记实体的上级行政区划简称添加到列表中
                lsanothername.append(mm["m.anothername"])
                # 将每个标记实体的编码添加到列表中
                lspac.append(mm["n.pac"])
            # 如果最小距离值小于10
            if minvalue <= 10:  # 阈值为10
                # 根据最小距离对应标记实体的上级行政区划简称，找到对应的编码
                nname_pac = str(lspac[lsanothername.index(minkey[0])])
                # Cypher查询标记实体的上级行政区划编码，返回标记实体的上级行政区划全称和上级行政区划的上级全称，相当于标记实体的上两级行政区划全称
                data_disname = graph.run(
                    "match(n:Province)-[:`隶属`]->(m:Province) where n.pac= '" + nname_pac + "' return m.name, n.name").data()
                # 如果j地名与标记实体的上级行政区划全称相同
                if j == data_disname[0]["n.name"]:
                    # 则j->m
                    result_4 = j + "->" + data_disname[0]["m.name"]
                else:
                    # 否则，j(n)->m
                    result_4 = j + "(" + data_disname[0]["n.name"] + ")" + "->" + data_disname[0]["m.name"]
                lsresult.append(result_4)  # 加入结果列表
                lsuntreated.remove(j)  # 从未处理中移除j
                lstreated.append(j)  # 向已处理中添加j
                dis += 1
            # 如果最小距离大于10
            else:
                lslevel = []  # 存储标记实体的等级
                lsnpac = []  # 存储标记实体的编码
                # 遍历标记实体列表
                for mm in ls_temp:
                    # 将每个标记实体的等级加入列表
                    lslevel.append(mm["n.level"])
                    # 将每个标记实体的编码加入列表
                    lsnpac.append(mm["n.pac"])
                # min_level=min(lslevel)
                # 找到列表中等级数最小的（对应等级是最高的）
                minlev_acount = lslevel.count(min(lslevel))
                # 如果等级=1
                if minlev_acount == 1:
                    # 根据最小等级数对应标记实体的等级数，找到对应的编码
                    nname_pac = str(lsnpac[lslevel.index(min(lslevel))])
                    # Cypher查询标记实体的编码，返回标记实体全称和标记实体上级行政区划的全称
                    data_disname = graph.run(
                        "match(n:Province)-[:`隶属`]->(m:Province) where n.pac= " + nname_pac + " return m.name, n.name").data()
                    # 如果该标记实体的全称与j地名相同
                    if data_disname[0]["n.name"] == j:
                        # j->m
                        result_5 = j + "->" + data_disname[0]["m.name"]
                    else:
                        # j(n)->m
                        result_5 = j + "(" + data_disname[0]["n.name"] + ")" + "->" + data_disname[0]["m.name"]
                    lsresult.append(result_5)  # 加入结果列表
                    lsuntreated.remove(j)  # 从未处理列表中移除j
                    lstreated.append(j)  # 在已处理列表中添加j
                    lel += 1  # 尺度法

        # 同名标识法（从已处理，也就是已标记实体出发，作为消岐依据）
        # for j in lsuntreated:
        #     # Cypher查询该地名的简称，返回该地名的全称和上级行政区划全称
        #     data_2 = graph.run(
        #         "match(n:Province)-[:`隶属`]->(m:Province) where n.anothername= '" + j + "'return n.name, m.name").data()
        #     ls_temp = []  # 标记实体的存储列表
        #     for m in data_2:
        #         # 如果全称n在已处理列表中，就将n加入标记实体列表
        #         if m['n.name'] in lstreated:
        #             ls_temp.append(m)
        #     # 如果标记实体的列表长度为1
        #     if len(ls_temp) == 1:
        #         # 结果为j简称（j全称）->上级实体
        #         result_2 = j + "(" + ls_temp[0]["n.name"] + ")" + "->" + ls_temp[0][
        #             "m.name"]  # 标识有结果，j(简称) n(全称)隶属于m(全称)
        #         lsresult.append(result_2)  # 加入结果列表
        #         lsuntreated.remove(j)  # 从未处理中删除j
        #         lstreated.append(j)  # 将j加入到已处理列表
        #         sign += 1  # 同名标识法数量

    # 尺度消歧
    for j in lsuntreated:
        # Cypher查询j的全称或简称，返回j全称，j上级行政区划全称和等级
        data_level2 = graph.run(
            "match(n:Province)-[:`隶属`]->(m:Province) where n.anothername= '" + j + "' or n.name= '" + j + "'return n.name, m.name, m.level").data()
        lslevel_nname = []
        lslevel_mname = []
        lslevel_mlevel = []
        for m in data_level2:
            lslevel_nname.append(m["n.name"])
            lslevel_mname.append(m["m.name"])
            lslevel_mlevel.append(m["m.level"])
        # 找到列表中等级数最小的（对应等级是最高的）
        minlevel_count = lslevel_mlevel.count(min(lslevel_mlevel))
        if minlevel_count == 1:
            min_level = min(lslevel_mlevel)
            # 找到等级数最高对应的j地名全称
            max_nname = lslevel_nname[lslevel_mlevel.index(min_level)]
            # 找到等级最高对应的j上级行政区划全称
            max_mname = lslevel_mname[lslevel_mlevel.index(min_level)]
            # 如果j和j地名全称相同
            if j == max_nname:
                result_5 = j + "->" + max_mname
            else:
                result_5 = j + "(" + max_nname + ")" + "->" + max_mname
            lsresult.append(result_5)
            lsuntreated.remove(j)
            lstreated.append(j)
            lel1 += 1
    # 模糊查询
    dim = []
    for j in lsuntreated:
        # Cypher查询j地名，返回j全称、简称，等级，上级行政区划全称、等级。
        data_3 = graph.run(
            "match(n:Province)-[:`隶属`]->(m:Province) where n.name=~'" + j + ".*'return n.name,n.anothername, m.name,m.level,n.level").data()
        # 如果查询结果不为空
        if len(data_3) > 0:
            ls_dim1 = []  # 所有查询结果的地名
            ls_dim2 = []  # j地名与已处理列表有关联
            ls_dimlevel = []
            # 遍历查询结果
            for m in data_3:
                # 如果j地名等级小于5
                if str(m["n.level"]) < str(5):
                    # 将该地名添加到ls_dim1列表中
                    ls_dim1.append(m)
            # 遍历ls_dim1列表
            for m7 in ls_dim1:
                # 如果j全称或者j简称在已处理列表中
                if m["n.name"] in lstreated or m["n.anothername"] in lstreated:
                    # 将j地名添加到ls_dim2中
                    ls_dim2.append(m)
                    # 在ls_dimlevel列表中添加上级行政区划的等级
                    ls_dimlevel.append(m["m.level"])
            # 如果列表长度为1
            if len(ls_dim1) == 1:
                # j(n)->m
                result = j + "(" + ls_dim1[0]["n.name"] + ")" + "->" + ls_dim1[0]["m.name"]
                lsresult.append(result)  # 加入结果列表
                lsuntreated.remove(j)  # 从未处理列表中移除j
                lstreated.append(j)  # 将j添加到已处理列表中
                # print(result_)
                dim.append(j)  # 将j添加到地名列表中
                anexact += 1
                # topo += 1
                print("精确匹配消岐",j)
            # 如果列表2（有关联）长度等于1
            elif len(ls_dim2) == 1:
                # 则j(n)->m
                result = j + "(" + ls_dim1[0]["n.name"] + ")" + "->" + ls_dim1[0]["m.name"]
                lsresult.append(result)
                lsuntreated.remove(j)
                lstreated.append(j)
                dim.append(j)
                sign += 1  # 标识法
                print("标识法消岐",j)
            # 如果大于1
            elif len(ls_dim2) > 1:
                # 选择等级最高的
                level_count = ls_dimlevel.count(min(ls_dimlevel))
                if level_count == 1:
                    # print("模糊查询：")
                    print(pd.DataFrame(ls_dim2))
                    result = j + "(" + ls_dim2.index(min(ls_dimlevel))["n.name"] + ")" + "->" + \
                             ls_dim2.index(min(ls_dimlevel))["m.name"]
                    dim.append(j)
                    lel += 1
                    # topo += 1
                    print("尺度消岐",j)
                    lsresult.append(result)
                    lsuntreated.remove(j)
                    lstreated.append(j)
    no1 = no1 + len(lsuntreated)
    no = no + len(lsunreatched)
    lsuntreat = lsuntreated + lsunreatched

    ws.write(i, 0, str(lsresult))  # 消歧的行政区划
    ws.write(i, 1, str(lsuntreated))  # 处理不了的行政区划
    ws.write(i, 2, str(lsunreatched))  # 查询不到的
    ws.write(i, 3, str(lsuntreat))  # 需要自然实体消歧的

ws.write(0, 0, "消歧的行政区划")  # 消歧的行政区划
ws.write(0, 1, "处理不了的行政区划")  # 处理不了的行政区划
ws.write(0, 2, "查询不到的")  # 查询不到的
ws.write(0, 3, "需要实体消歧的")  # 查询不到的
print("共处理摘要篇数：", allnrows)
print("涉及地名总数：", allwords)
print("聚类篇章数", placename_cluster)
print("没有歧义自然实体的摘要篇数：", exactnrows)
print('准确（无需消歧）：', exact)
print("精确匹配：", anexact)
print('词间距：', updown)
# print("词间距：", dis)
print("标识：", sign+dis)
print("标识法中的尺度法：",lel)
print('尺度：', lel1)
print('拓扑关系：', topo)

# print('上下级：', updown)

print('查询不到：', no)
print('消歧无果：', no1)
wb.save(filename)

time_end = time.time()  # 记录结束时间
time_sum = time_end - time_start  # 计算的时间差为程序的执行时间，单位秒/s
print(time_sum)
