import struct
import sqlite3
import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
import os

msgfiledir = 'msgfiles/'
# 필수 def
def min_diff_pos(array_like, target, startIdx):
    return startIdx+np.abs(np.array(array_like)[startIdx:]-target).argmin()

def unpack(typ, data):
    ans = False
    
    if typ == 'f':
        ans = struct.unpack('f', data)[0]
    elif typ == 'd':
        ans = struct.unpack('d', data)[0]
    
    return ans

def mkfolder(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        
def mkfile(dirname, fname, data):
    with open(dirname+fname+'txt','rb') as f:
        f.write(data)

# msg 파일 unpack 코드 연관

def checkmsg(dirname, msgname):
    with open(dirname + msgname, 'r') as f:
        msgdata = f.readlines()
        msgname = []
        for msg in msgdata:
            msgname.append(msg.split()[0])
    
    return decryptpos(msgname)

# data type check
def decryptpos(msgname):
    msgsize = {'std_msgs/Header' : ['header',28],
                'geometry_msgs/Vector3' : ['ddd',[8,8,8]],
                'InsGnssSummaryT': ['fffff',20], # 크기를 알 수 없음
                'float32' : ['f',4],
                'float64' : ['d',8],
                'uint8' : ['B',1],
                'uint32' : ['I',4],
                'bool' : ['?',1]}
    keys = []
    for name in msgname:
        keys.append(msgsize[name])
    
    return keys

# bag 읽어오기
db_name = '/ros_data/rosbag2_2021_04_15-16_34_39_0.db3'
db_name = db_name.split('.')[0]
con = sqlite3.connect('/home/krri/bag_check/rosbag2_2021_04_15-16_34_39/rosbag2_2021_04_15-16_34_39_0.db3')

df_topic = pd.read_sql_query("SELECT id, type from topics WHERE type='custom_msg/msg/Vn300'", con)
df_msg = pd.read_sql_query("SELECT * from messages", con)


topic_header = ['id','type']
msg_header = ['topic_id','timestamp','data']

topic_df = df_topic[topic_header].values
fname = df_topic['type'].values
msg_df = df_msg[msg_header].values

tmp_df = [[] for i in fname]

for i_index, i in enumerate(topic_df):
    for j in msg_df:
        if i[0] == j[0]:
            tmp_df[i_index].append(j)
            
# msg 파일 선택
# 폴더에서 전부 긁어와서 리스트에 저장
key = checkmsg(msgfiledir,'Vn300.msg')

chk = ''
for i in key[1:]:
    chk += i[0]
chkdata = []
# 데이터 unpack and 저장
for i in tmp_df:
    for j in i:
        # print(j[2])
        # chkdata.append(np.rad2deg(struct.unpack(chk[6:9],j[2][32:44])))
        chkdata.append(struct.unpack(chk, j[2][28:]))
        
np.savetxt('chkdata.txt', chkdata, fmt='%s')

# 데이터 폴더 생성 및 저장
# mkfolder(db_name)
# for i_index, i in enumerate(fname):
#     i = i.split('/')[-1]
#     tmp = np.array(tmp_df[i_index])
#     np.savetxt(db_name+'/'+i+'.txt', tmp, fmt='%s')


'''
df_top = pd.read_sql_query("SELECT * from topics", con)
# bag id 확인
honeyId = df_top[df_top['type'].isin(['hg_nav_node/msg/Msg6405'])]['id'].values[0]
vn300Id = df_top[df_top['type'].isin(['custom_msg/msg/Vn300'])]['id'].values[0]

df = pd.read_sql_query("SELECT * from messages", con)

df[df['topic_id'].isin([honeyId])]

#vn300 parsing
vn300 = df[df['topic_id'].isin([vn300Id])]
vn300Arr = []

for i in range(len(vn300)):
    
    selRow = vn300[i:i+1].data
    print(len(selRow.values.tolist()[0][:]))
    print(selRow.values.tolist()[0][:])
    
    time = vn300[i:i+1].timestamp.tolist()[0]/1000000000
    
    tempData = selRow.values.tolist()[0][28:28+8]
    print(tempData)
    roll = unpack('d',tempData)
    
    tempData = selRow.values.tolist()[0][28+8:28+16]
    pitch = unpack('d',tempData)
    print(tempData)
    
    tempData = selRow.values.tolist()[0][28+16:28+24]
    yaw = unpack('d',tempData)
    print(tempData)
    
    vn300Arr.append([time, roll, pitch, yaw])

# honeywell parsing
honey = df[df['topic_id'].isin([honeyId])]
honeyArr = []

startbit=32+4

for i in range(len(honey)):
    
    selRow = honey[i:i+1].data
    
    time = honey[i:i+1].timestamp.tolist()[0]/1000000000
    
    tempData = selRow.values.tolist()[0][startbit:startbit+4]
    roll = np.rad2deg(unpack('f',tempData))
    
    tempData = selRow.values.tolist()[0][startbit+4:startbit+8]
    pitch = np.rad2deg(unpack('f',tempData))
    
    tempData = selRow.values.tolist()[0][startbit+8:startbit+12]
    yaw = np.rad2deg(unpack('f',tempData))
    
    honeyArr.append([time, roll, pitch, yaw])

# make timesync array
curTime = vn300Arr[0][0] #current time? why v300Arr first time?
vn300npArr = np.array(vn300Arr)
vn300TimeArr = vn300npArr[:,0]
vm300TimeIdx = 0

honeynpArr = np.array(honeyArr)
honeyTimeArr = honeynpArr[:,0]
honeyTimeIdx = 0

vm300TimeIdx = min_diff_pos(vn300TimeArr, curTime, 0)
honeyTimeIdx = min_diff_pos(honeyTimeArr, curTime, 0)


newDataArr = []
vmIdxArr= []
honeyIdxArr=[]

for i in range(len(vn300Arr)):
    vm300TimeIdx = min_diff_pos(vn300TimeArr, curTime, vm300TimeIdx)
    honeyTimeIdx = min_diff_pos(honeyTimeArr, curTime, honeyTimeIdx)        
    
    vmIdxArr.append(vm300TimeIdx)
    honeyIdxArr.append(honeyTimeIdx)
    
    newDataArr.append( [curTime, vn300Arr[vm300TimeIdx][3], honeyArr[honeyTimeIdx][3] ] )
    
    curTime += 0.01 
    
# offset error delete
newDataArr = np.array(newDataArr)

error = newDataArr[:,2]-newDataArr[:,1]
median = np.median(error)

plt.plot(error-median)
plt.show()
'''