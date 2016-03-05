# -*- coding: utf-8 -*-

import warnings
from apscheduler.schedulers.blocking import BlockingScheduler
from sklearn import svm
import scipy as sp
from pymongo import MongoClient

import threading

import json
from socketIO_client import SocketIO, LoggingNamespace

import datetime
import time

warnings.filterwarnings("ignore")

label = []
day = [[],[],[],[],[],[],[]]
gateway_id = "0013A200409FD1AB"
device_id = "0013A20040B5769A"

# Mongo DB 연결 및 데이터 베이스 선택
client = MongoClient('mongodb://220.67.128.34:3306')
db = client['Chang']

# 모든 Label이 한 가지 데이터로 이루어졌을 때 예외 처리
# 마지막에 의미 없는 데이터를 넣어준다.
def exceptionData(excedata) :

    tcount = 0
    fcount = 0
    for temp in excedata[1] :
        if temp == True :
            tcount = tcount + 1
        else :
            fcount = fcount + 1

    if tcount == len(excedata[1]) :
        excedata[0].append([99999999, 0.9999, 9])
        excedata[1].append(0)
    elif fcount == len(excedata[1]) :
        excedata[0].append([99999999, 0.9999, 9])
        excedata[1].append(1)

    return excedata

# Mongo DB에서 받아온 데이터에서 날짜를 추출하는 함수
def extraDate(datedata) :
    datedata = str(datedata)
    datestr = ""
    for datetemp in datedata[:datedata.find(" ")].split("-") :
        datestr = datestr + str(datetemp)
    return datestr

# Mongo DB에서 받아온 데이터에서 시간를 추출하고 비율로 변환하는 함수
def extraTime(datedata) :
    datedata = str(datedata)
    timearray =  datedata[datedata.find(" ") : datedata.find(".")].split(":")
    time = (int(timearray[0]) * 3600) + (int(timearray[1]) * 60) + (int(timearray[2]))
    return time/float(86400)

# Mongo DB에서 읽어온 데이터에서 원하는 값을 추출 후
# Feature 데이터(List)와 Label 데이터(List) 생성
def makeLabel(socketcount, socketnum, dayindex, yestertime, filenamelist, writeop, staylist) :

    ontooffchange = True
    offtoonchange = True
    currentchange = True
    zerocurrentchange = True
    onofftime1 = 0.0
    onofftime2 = 0.0
    currenttime = 0.0
    zerocurrenttime = 0.0
    testtemp1 = ""
    testtemp2 = ""
    testcurrenttemp = ""
    testzerocurrenttemp = ""
    doccount = 0
    currentac1 = 0
    currentac2 = 0
    currentac3 = 0
    cursor = db.test_datas.find()
    stay = "1"

    f = open(filenamelist[0], writeop)
    lf = open(filenamelist[1], writeop)
    pf = open(filenamelist[2], writeop)

    for document in cursor:

        if (len(document['socket']) == socketcount) and ((extraDate(document['date'])) == yestertime) :
            # Stay를 판별해 데이터를 기록
            stayindex = 0
            for staytime in staylist :
                if document['date'] < staytime["time"] :
                    if staytime["stay"] :
                        stay = "0"
                        break
                    else :
                        stay = "1"
                        break
                stayindex = stayindex + 1

            if staylist[stayindex - 1]["time"] <= document['date'] :
                stay = str(int(staylist[stayindex - 1]["stay"]))

            doccount = doccount + 1
            f.write(extraDate(document['date']) + "\t" + str(extraTime(document['date'])) + "\t" + stay + "\t")

            # State가 켜져 있고 콘센트를 사용 중에서 콘센트를 사용 하지 않는 상태가 되었을 때까지의 시간을 체크
            # State가 0인 상태에서 1이 되었을 때의 시간을 체크
            if document['socket'][socketnum-1]['state'] == 1 :
                f.write("1\n")

                if document['socket'][socketnum-1]['current'] > 0 :
                    currentac1 = currentac1 + 1
                    if currentchange == True :
                        currenttime = extraTime(document['date'])
                        testcurrenttemp = extraDate(document['date']) + "\t" + str(extraTime(document['date'])) + "\t" + "1"
                        currentchange = False
                    if ontooffchange == True :
                        onofftime1 = extraTime(document['date'])
                        testtemp1 = extraDate(document['date']) + "\t" + str(extraTime(document['date'])) + "\t" + "1"
                        ontooffchange = False

                    if zerocurrentchange == False :

                        if extraTime(document['date']) - zerocurrenttime > 0.083 :
                            pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testzerocurrenttemp + "\t" + stay + "\n")
                            zerocurrentchange = True


                else :
                    currentac3 = currentac3 + 1
                    if zerocurrentchange == True :
                        zerocurrenttime = extraTime(document['date'])
                        testzerocurrenttemp = extraDate(document['date']) + "\t" + str(extraTime(document['date'])) + "\t" + "0"
                        zerocurrentchange = False

                    if currentchange == False :
                        if extraTime(document['date']) - currenttime > 0.083 :
                            pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testcurrenttemp + "\t" + stay + "\n")
                            currentchange = True

                if offtoonchange == False :
                    if extraTime(document['date']) - onofftime2  > 0.083 :
                        pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testtemp2 + "\t" + stay + "\n")
                        offtoonchange= True

            # State가 1인 상태에서 0이 되었을 때의 시간을 체크
            else :
                f.write("0\n")
                currentac2 = currentac2 + 1
                if ontooffchange == False :
                    if extraTime(document['date']) - onofftime1 > 0.083 :
                        pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testtemp1 + "\t" + stay + "\n")
                        ontooffchange = True
                if offtoonchange == True :
                    onofftime2 = extraTime(document['date'])
                    testtemp2 = extraDate(document['date']) + "\t" + str(extraTime(document['date'])) + "\t" + "0"
                    offtoonchange = False

            # Label List 생성. State가 1이고 콘센트를 사용 중인 경우는 True
            # State가 0이거나 콘센트를 사용하지 않는 경우는 False

            if (document['socket'][socketnum-1]['current']) > 0 and (document['socket'][socketnum-1]['state'] == True) :
                lf.write("1\n")
            else :
                lf.write("0\n")

    f.close()
    lf.close()

    # 하루동안 같은 상태로 유지되었을 때를 기록
    if currentac1 == doccount :
        pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testtemp1 + "\t" + stay + "\n")
    elif currentac2 == doccount :
        pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testtemp2 + "\t" + stay + "\n")
    elif currentac3 == doccount :
        pf.write(str(socketnum-1) + "\t" + dayindex + " \t " + testzerocurrenttemp + "\t" + stay + "\n")

    pf.close()


# 지난 달 데이터를 읽어오는 함수
def readData(featurefile, labelfile, patternfile) :
    totaldata = []
    featuredata = sp.genfromtxt(featurefile, delimiter="\t")
    featurelist = featuredata.tolist()
    totaldata.append(featurelist)
    labeldata = sp.genfromtxt(labelfile, delimiter="\n")
    labellist = labeldata.tolist()
    totaldata.append(labellist)

    patterndata = sp.genfromtxt(patternfile, delimiter="\t")
    patternlist = patterndata.tolist()
    totaldata.append(patternlist)

    return totaldata

# 현재 요일에 대한 패턴을 만드는 함수
def today_pattern(todayindex, patterndata, scount) :
    tempresult = []
    for t in range(scount) :
        for w in patterndata[t] :
            tmp = []
            if w[1] == todayindex :
                tmp.append(w[3]) # 시간
                tmp.append(w[0]) # 소켓 번호
                tmp.append(w[4]) # state
                tmp.append(w[5]) # stay
            if len(tmp) != 0 :
                tempresult.append(tmp)
    tempresult.sort()
    return tempresult

# 소켓 갯수에 맞게 파일을 생성
def make_fileList(sockcount) :
    temptotalfilelist = []
    for ffm in range(socketcount) :
        secondtemplist = []

        for sfm in range(2) :
            thirdtemplist = []

            for tfm  in range(3) :
                if tfm == 0 :
                    thirdtemplist.append("sock" + str(ffm+1) + "dataset" + str(sfm+1) + ".tsv")
                elif tfm == 1 :
                    thirdtemplist.append("sock" + str(ffm+1) + "labeldata" + str(sfm+1) + ".txt")
                elif tfm == 2 :
                    thirdtemplist.append("sock" + str(ffm+1) + "daypattern" + str(sfm+1) + ".txt")
            secondtemplist.append(thirdtemplist)

        temptotalfilelist.append(secondtemplist)
    return temptotalfilelist

# 분류 결과 값을 서버로 보내고 사용자가 제어를 하였는지 무시하였는지를 판단해 기록하는 함수
def predictDataHandling(checkh, checkm, checks, socknumber, testpredicttime, teststate, teststayvalue, predictresultvalue, predictfilename) :

    testcursor = db.test_datas.find()
    usermind = []
    for p in testcursor :
        if (p["date"].year == time.localtime().tm_year) and (p["date"].month == time.localtime().tm_mon) and (p["date"].day == time.localtime().tm_mday) :
            if (p["date"].hour == checkh) and (p["date"].minute == checkm) and (p["date"].second == checks):
                usermind.append(p)
                break
    # 현재 데이터 베이스의 값과 2분 후 데이터 베이스를 비교해 제이거 되었는지 확인한다.
    if len(usermind) != 0 :
        if usermind[0]["socket"][socknumber]["state"] != teststate :
            time.sleep(120)
            retlist = []
            testcursor = db.test_datas.find()
            for p in testcursor :
                if (p["date"].year == time.localtime().tm_year) and (p["date"].month == time.localtime().tm_mon) and (p["date"].day == time.localtime().tm_mday) :
                    if (p["date"].hour == checkh) and (p["date"].minute == checkm + 2) and (p["date"].second == checks):
                        retlist.append(p)
                        break
            if retlist[0]["socket"][socknumber]["state"] != teststate :
                tempdf = open(predictfilename[0], 'a')
                templf = open(predictfilename[1], 'a')
                tempdf.write(str(extraDate(retlist[0]["date"])) + "\t" + str(testpredicttime) + "\t" + str(teststayvalue) + "\t" + str(teststate) + "\n")
                if predictresultvalue :
                    templf.write("0\n")
                else :
                    templf.write("1\n")


##################################################################

clf = svm.SVC()
socketcount = 4

totalfilelist = []
totalfilelist = make_fileList(socketcount)


totalfeaturelabel = [[], [], [], []]
totalpattern = [[], [], [], []]

index = 0
indexlast = True
h = 1
m = 1
s = 1
state = 0
socknum = 0
ustay = 0
print("\n[ Start Personalization ]")

# 스케줄러를 이용해 실시간으로 데이터를 누적하고
# 사용자 패턴에 맞는 시간에 분류기를 수행하여 제어를 요청한다.
while(True) :

    sched = BlockingScheduler()

    # 00시 00분 00초에 전날 데이터를 기록하고 새로운 달이 되면 지난 달 데이터를 읽어온다.
    @sched.scheduled_job('cron', hour=3, minute=28, second = 0)
    def scheduled_job():
        sched.add_job(train_classifer,"interval", seconds=2, id="trainclassifier")

    def train_classifer() :
        print("Making training data...")
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(1)
        yesdate = str(yesterday.year) + str(yesterday.month) + str(yesterday.day)


        global index
        index = 0

        # 해당 유저의 stay 변화 기록을 가져와 배열로 만든다.
        checkcursor = db.users.find_one({"email" : "piw0223@gmail.com"})
        stayarray = []
        for userstay in checkcursor["range"] :
            if (userstay["time"].year == yesterday.year) and (userstay["time"].month == yesterday.month) and (userstay["time"].day == 26) :
                stayarray.append(userstay)

        flag = int(yesterday.month)
        if flag % 2 == 0 :
            flag = 1
        else :
            flag = 0


        datalabel = [[], [], [], []]
        # 새로운 달이 되었을 때 지난 달 데이터를 읽어온다.
        if today.day == 1 :
            for u in range(socketcount) :
                datalabel[u] = readData(totalfilelist[u][flag][0], totalfilelist[u][flag][1], totalfilelist[u][flag][2])
                datalabel[u] = exceptionData(datalabel[u])

            for u in range(socketcount) :
                if flag == 0 :
                    makeLabel(socketcount, u+1, str(yesterday.weekday()), yesdate, totalfilelist[u][1], 'w', stayarray)
                else :
                    makeLabel(socketcount, u+1, str(yesterday.weekday()), yesdate, totalfilelist[u][0], 'w', stayarray)
        # 1일이 아닌 다른 날일 때는 전날 데이터를 읽어와 기록한다.
        else :
            for u in range(socketcount) :
                if flag == 0 :
                    makeLabel(socketcount, u+1, str(yesterday.weekday()), yesdate, totalfilelist[u][1], 'a', stayarray)
                else :
                    makeLabel(socketcount, u+1, str(yesterday.weekday()), yesdate, totalfilelist[u][0], 'a', stayarray)

        # 데이터 유무 예외 처리
        if len(datalabel[0]) == 0 :
            for u in range(socketcount) :
                datalabel[u] = readData(totalfilelist[u][flag][0], totalfilelist[u][flag][1], totalfilelist[u][flag][2])
                if len(datalabel[u][0]) == 0 :
                    print("Socket number " + str(u) + " : Not exist data of last month")
                    break
                datalabel[u] = exceptionData(datalabel[u])

        for u in range(socketcount) :
            totalfeaturelabel[u] = datalabel[u]
            totalpattern[u] = datalabel[u][2]
        sched.remove_job("trainclassifier")
        sched.shutdown(False)

    # 오늘의 패턴을 만들고 현재 시간보다 뒤에 일어날 일만 수행한다.
    if len(today_pattern(time.localtime().tm_wday, totalpattern, socketcount)) != 0 :
        temptime = today_pattern(time.localtime().tm_wday, totalpattern, socketcount)
        if index < len(temptime) :
            now = time.localtime()
            p = 0
            if indexlast == True :
                for t in temptime :

                    th = int(t[0] * float(86400) / float(3600))
                    tempm = (t[0] * float(86400) / float(3600)) - h
                    tm = int(tempm * 60)
                    ts = int((tempm * 60 - m) * 60)
                    if (int(now.tm_hour) <= th) and (int(now.tm_min) <= tm) and (int(now.tm_sec) < ts) :
                        index = p
                        indexlast = False
                        break
                    p = p + 1
            if p < len(temptime) :
                # 시간을 갱신하면서 다음 스케줄에 수행될 시간을 세팅한다.
                h = int(temptime[index][0] * float(86400) / float(3600))
                tempm = (temptime[index][0] * float(86400) / float(3600)) - h
                m = int(tempm * 60)
                s = int((tempm * 60 - m) * 60)

                state = temptime[index][2]
                socknum = int(temptime[index][1])
                ustay = temptime[index][3]
                print(socknum, time.localtime().tm_wday, temptime[index][0])
                index = index + 1
                if index == len(temptime) :
                    indexlast = True
                print("\nNext Pattern==================")
                print("Next test socket number : " + str(socknum))
                print("Next running : " + str(h) + " : " + str(m) + " : " + str(s))
                if ustay :
                    print("Next test stay : on")
                else :
                    print("Next test stay : off")
                if state == 1 :
                    print("Goal state : True")
                else :
                    print("Goal state : False")

            else :
                print("\n==============================================")
                print("Today's schedule end ! Next running 00 : 00 : 00")
        else :
            print("\n==============================================")
            print("Today's schedule end ! Next running 00 : 00 : 00")



    @sched.scheduled_job('cron', hour = h, minute = m, second = s)
    def scheduled_job():
        sched.add_job(test_classifier, "interval", seconds=2, id="testclassifier")

    def test_classifier () :

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(1)
        flag = int(yesterday.month % 2)
        if flag % 2 == 0 :
            flag = 0
        else :
            flag = 1

        if (len(totalfeaturelabel[socknum]) == 0) :
            print("Not exist multi tab socket data")

        else:

            # 데이터 Exception 처리
            testdata = exceptionData(totalfeaturelabel[socknum])

            if len(testdata[0]) > len(testdata[1]) :
                for i in range(len(testdata[0]) > len(testdata[1])) :
                    testdata[1].append(testdata[1][len(testdata[1]) - 1])


            elif len(testdata[0]) < len(testdata[1]) :
                for i in range(len(testdata[1]) - len(testdata[0])) :
                    testdata[0].append([99999999, 0.9999, 9])
                    clf.fit(testdata[0],testdata[1])

            # 분류기 훈련
            print("\nNow Running====================")
            print("Classifier Running " + str(h) + " : " + str(m) + " : " + str(s))
            print("Training classifier...")
            clf.fit(testdata[0],testdata[1])

            todaytest = datetime.date.today()
            todaydate = str(todaytest.year) + str(todaytest.month) + str(todaytest.day)
            testtime = ((h * 3600) + (m * 60) + s) / float(86400)

            # 테스트 데이터 분류 수행
            predictresult = clf.predict([int(todaydate), testtime, ustay, state])

            # 분류기 평가
            evaluation_score = clf.score([int(todaydate), testtime, ustay, state], predictresult)
            resultstate = bool(predictresult[0])

            print("Predict result : " + str(predictresult[0]))
            print("Predict evaluation score : " + str(evaluation_score))


            # 제어를 위해 분류 결과를 서버로 보내는 부분
            import socket

            predictth = threading.Thread(target=predictDataHandling, args=(h, m, s, socknum, testtime, state, ustay, resultstate, totalfilelist[socknum][flag]))

            try :
                exceptsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                exceptsock.connect(("220.67.128.34", 8080))
                exceptsock.close()
                socket = SocketIO('http://220.67.128.34', 8080, LoggingNamespace)
                socket.emit('classifier_Message', json.dumps({'gatewayId': gateway_id, "deviceId" : device_id, "socketNumber" : socknum, "state" : resultstate}))

                predictth.start()
                predictth.join()

            except Exception :
                print("* Control Fail : Server off")

            sched.remove_job("testclassifier")
            sched.shutdown(False)

    sched.start()


# testcursor = db.test_datas.find()
# qq = []
# for p in testcursor :
#     print(extraDate(p["date"]))
#     print(extraTime(p["date"]))
#     if (p["date"].year == time.localtime().tm_year) and (p["date"].month == time.localtime().tm_mon) and (p["date"].day == 24) :
#         qq.append(p)
#         break
#
# print(extraDate(qq[0]["date"]))
# k = 1.
# resultstate = bool(k)

# try :
#
#     exceptsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     exceptsock.connect(("220.67.128.34", 8080))
#     exceptsock.close()
#     socket = SocketIO('http://220.67.128.34', 8080, LoggingNamespace)
#     socket.emit('classifier_Message', json.dumps({'gatewayId': gateway_id, "deviceId" : device_id, "socketNumber" : socknum, "socketState" : resultstate}))
#
# except Exception:
#     print("Server off")




# datalabel = [[], [], [], []]
#
# for u in range(socketcount) :
#     datalabel[u] = readData(totalfilelist[u][0][0], totalfilelist[u][0][1], totalfilelist[u][0][2])
#     datalabel[u] = exceptionData(datalabel[u])
# print(datalabel[0][0])


# tt = [[],[],[],[]]
# for u in range(socketcount) :
#     tt[u] = datalabel[u][2]
# today_pattern(0, tt, 4)



# uf = open("users.txt", 'a')
# usercursor = db.users.find()
# tempcount = 0
# for doc in usercursor :
#     tempcount = tempcount + 1
#     print(tempcount)
#
# usercursor = db.users.find()
# if tempcount != len(users) :
#     for doc in usercursor :
#         if users.__contains__(doc['device'][0]['deviceId']) == False:
#             users.append(doc['device'][0]['deviceId'])
#             uf.write(str(doc['device'][0]['deviceId']) + "\n")
# uf.close()
# print(users)


# today = datetime.date.today()
# yesterday = today - datetime.timedelta(1)
# yesdate = str(yesterday.year) + str(yesterday.month) + str(yesterday.day)
#
# print(yesterday.weekday())
# checkcursor = db.users.find_one({"email" : "1"})
# stayarray = []
# for userstay in checkcursor["range"] :
#     if (yesterday.year == userstay["time"].year) and (userstay["time"].month == 11) and (userstay["time"].day == 25) :
#         stayarray.append(userstay)

#
#
# for u in range(socketcount) :
#     makeLabel(socketcount, u+1, str(yesterday.weekday()), yesdate, totalfilelist[u][1], 'w')

# makeLabel(4, 1, str(yesterday.weekday()), "20151116", filelist[0], 'w')
# totalp = [[], [], [], []]
# datalabel = [[], [], [], []]
# for u in range(socketcount) :
#     datalabel[u] = readData(totalfilelist[u][0][0], totalfilelist[u][0][1], totalfilelist[u][0][2])
#     datalabel[u] = exceptionData(datalabel[u])
#     totalp[u] = datalabel[u][2]
#
# today_pattern(today.weekday(), totalp, 4)

# makeLabel(socketcount, 4, str(yesterday.weekday()), "20151125", totalfilelist[3][0], 'a', stayarray)

# if len(datalabel[0]) == 0 :
#     print("Not exist multi tab")
#
# else:
#     # print(len(datalabel[0]))
#     # print(len(datalabel[1]))
#     # 데이터 Exception 처리
#     datalabel = exceptionData(datalabel)
#
#
#     if len(datalabel[0]) > len(datalabel[1]) :
#         for i in range(len(datalabel[0]) > len(datalabel[1])) :
#             datalabel[1].append(datalabel[1][len(datalabel[1]) - 1])
#
#
#     elif len(datalabel[0]) < len(datalabel[1]) :
#         for i in range(len(datalabel[1]) - len(datalabel[0])) :
#             datalabel[0].append([99999999, 0.9999, 9])
#             clf.fit(datalabel[0],datalabel[1])
#
#     print(datalabel[2])
#
#     # 분류기 훈련
#     clf.fit(datalabel[0],datalabel[1])
#
#     # 테스트 데이터 분류 수행
#     print("Predict", clf.predict([20151119,0.4753587962962963,1]))
# ##################################################

