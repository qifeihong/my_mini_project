#-*- coding=utf-8'*-
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
import os
import sys

# 解决写入文件时中文编码问题
reload(sys)
sys.setdefaultencoding('utf-8')

es = Elasticsearch(["http://**.**.**.**:9210"])
# 查看连接是否成功
print(es.info())

# 索引名
indexName = "company_info"

# 输出文件路径+文件名, 路径选择当前项目路径
fileName = "es_out.csv"
filePath = os.getcwd()
print("current file path:",filePath)
file = filePath + "/" + fileName

def printCsv(file, dataList):
    # 防止出现空数组
    if (len(dataList) == 0):
        return
    
    csvFile = ""
    writer = ""

    # 文件是否存在
    ifExist = os.path.exists(file)

    if (ifExist):
        # 文件存在, 则在后面追加
        print ("file already exist:", file)
        csvFile = open(file,'a+')
    else:
        # 文件不存在, 则新建文件
        csvFile = open(file,'wb')

    try:
        writer=csv.writer(csvFile)
        if (ifExist == False):
            # 如果是新建的表, 此处定义表的列名信息
            writer.writerow(('公司Id','公司名','地址','手机'))

        # 此处循环写入表数据
        for data in dataList:
            companyId = data['_source']['companyId'] if (data['_source'].has_key('companyId')) else 0
            name = data['_source']['name'] if (data['_source'].has_key('name')) else '--'
            a = data['_source'].has_key('address')
            address = data['_source']['address'] if (data['_source'].has_key('address')) else '--'
            telephone = data['_source']['telephone'] if (data['_source'].has_key('telephone')) else '--'

            # dataStr = "%d,%s,%s,%s" % (companyId, name, address, telephone)
            # print (dataStr)
            writer.writerow((companyId, name, address, telephone))

    except IOError as e:
        print ("IOError happen in:", e)
    except Exception as e:
        print ("Error happen in:", e)
    finally:
        csvFile.close()


def buildQueryBody(pageNo, pageSize):
    # 由于ES限制, 当偏移量>10000时需要特殊处理, 此处略
    offset = (pageNo -1) * pageSize
    body = {
        "query":{
            "bool":{
                "must":[
                    {
                        "range":{
                            "companyId":{
                                "gt":"200"
                            }
                        }
                    }
                ],
                "must_not":[

                ],
                "should":[

                ]
            }
        },
        "from":offset,
        "size":pageSize,
        "sort":[
            {"companyId":{ "order": "asc" }}
        ],
        "aggs":{

        }
    }
    return body


def main():
    print ("start...")

    initPageNo = 1
    initPageSize = 50

    # 定义一个兜底循环控制, 防止出现死循环. loopMaxCount值视情况决定
    loopCount = 0
    loopMaxCount = 1000
    while True:
        # 创建查询条件
        body = buildQueryBody(initPageNo, initPageSize)
        # 获取查询结果
        res = es.search(index=indexName, body=body)
        # seccessful == 1则成功
        ifSuccess = res['_shards']['successful']
        if (ifSuccess == 1):
            print ("success query %d times!" % initPageNo)
            resultList = res['hits']['hits']
            total = res['hits']['total']
            # 查询的数量, 用于判断是否继续查询
            resultSize = len(resultList)
            print ("total: %d, get result: %d" % (total, resultSize))

            # 打印输出结果
            printCsv(file, resultList)
            
            if (resultSize < initPageSize):
                print ("查询结束")
                break
            else:
                print ("下一页, 继续")
                initPageNo += 1
        else:
            print ("执行查询失败!", res)

        loopCount += 1
        if (loopCount >= loopMaxCount):
            break

    print ("end...")
    
if __name__ == '__main__':
    main()