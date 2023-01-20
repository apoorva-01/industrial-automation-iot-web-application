import os
from flask_cors import CORS, cross_origin
from flask import Flask, request, Response,jsonify
import pandas as pd
import moment
from datetime import datetime 
from bson import ObjectId
from gevent.pywsgi import WSGIServer
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
import pymongo
import time
import json
myclient = pymongo.MongoClient("mongodb://igscs:IGSCS041173WelcomeChirpstack@139.59.88.80:27017")
mydb = myclient["next-marelli"]
entriesCollection = mydb["entries"]

zero_timestamp = time.time()
ist_timestamp = zero_timestamp + 5*60*60+30*60
offset = datetime.fromtimestamp(ist_timestamp) - datetime.utcfromtimestamp(zero_timestamp)

    
def Average(lst):
    return sum(lst) / len(lst)




def filterTempfunc(e):
    temperatureArr = getTempHour(e.temperature, e.timestamp)
    return temperatureArr
def getTempHour(temp, time):
    hr = time.hour
    return { 'time': int(hr),'temperature': float(temp) }



def filterHumfunc(e):
    humidityArr = getHumHour(e.humidity, e.timestamp)
    return humidityArr
def getHumHour(hum, time):
    hr = time.hour
    return { 'time': int(hr),'humidity': float(hum) }


def GetTempHourlyData(e):
    tempDF=pd.DataFrame(e)
    ret = []
    for hcounter in range(24):
        df_query=tempDF[(tempDF.time==hcounter)]
        hdata = df_query['temperature']
        if(len(df_query.index)==0):
          ret.append({
          "hour": hcounter, "min": None,
          "max": None, "avg": None})
          continue
        ret.append({
        "hour": hcounter, "min":"{:.2f}".format(min(hdata)),
        "max":"{:.2f}".format(max(hdata)), "avg":"{:.2f}".format(Average(hdata))
        })
    ret_df=pd.DataFrame(ret)
    minArray=ret_df['min'].to_numpy().tolist()
    maxArray=ret_df['max'].to_numpy().tolist()
    avgArray=ret_df['avg'].to_numpy().tolist()

    return {'minArray':minArray, 'maxArray':maxArray,'avgArray':avgArray}


def GetHumHourlyData(e):
    humDF=pd.DataFrame(e)
    ret = []
    for hcounter in range(24):
        df_query=humDF[(humDF.time==hcounter)]
        hdata = df_query['humidity']
        if(len(df_query.index)==0):
          ret.append({
          "hour": hcounter, "min": None,
          "max": None, "avg": None})
          continue
        ret.append({
        "hour": hcounter, "min":"{:.2f}".format(min(hdata)),
        "max":"{:.2f}".format(max(hdata)), "avg":"{:.2f}".format(Average(hdata))
        })
    ret_df=pd.DataFrame(ret)
    minArray=ret_df['min'].to_numpy().tolist()
    maxArray=ret_df['max'].to_numpy().tolist()
    avgArray=ret_df['avg'].to_numpy().tolist()
    return {'minArray':minArray, 'maxArray':maxArray,'avgArray':avgArray}





@app.route("/",methods=['POST'])
def hello_world():
    entries_object=list(entriesCollection.find({'devEUI':request.json['deviceEUI']}))
    df=pd.DataFrame(entries_object)
    df['timestamp'] = df['timestamp'].apply(lambda x: x+offset)
    df_query=df[(df.timestamp >= datetime(int(request.json['start_date'][:4]),int(request.json['start_date'][5:7]),int(request.json['start_date'][8:10]))) & (df.timestamp <= datetime(int(request.json['end_date'][:4]),int(request.json['end_date'][5:7]),int(request.json['end_date'][8:10])))]
    
    tempArr  = df_query.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr  = df_query.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    if(len(humArr)>0 and len(tempArr)>0):
        hourlyTempData=GetTempHourlyData(tempArr)
        hourlyHumData=GetHumHourlyData(humArr)  
    else:
        hourlyTempData=[]
        hourlyHumData=[]


    return jsonify({"tempData": hourlyTempData,"humData": hourlyHumData}), 200



@app.route("/all-devices-comparison",methods=['POST'])
def comparison():
    entries_object=list(entriesCollection.find({}))
    df=pd.DataFrame(entries_object)
    df['timestamp'] = df['timestamp'].apply(lambda x: x+offset)
    df_query=df[(df.timestamp >= datetime(int(request.json['start_date'][:4]),int(request.json['start_date'][5:7]),int(request.json['start_date'][8:10]))) & (df.timestamp <= datetime(int(request.json['end_date'][:4]),int(request.json['end_date'][5:7]),int(request.json['end_date'][8:10])))]
    
    
    df_query7a0a=df_query[(df_query.devEUI =='a84041b931837a0a')]
    df_query7a01=df_query[(df_query.devEUI =='a840417eb1837a01')]
    df_query79fd=df_query[(df_query.devEUI =='a8404181e18379fd')]
    df_query7a0e=df_query[(df_query.devEUI =='a8404152a1837a0e')]
    df_query79f9=df_query[(df_query.devEUI =='a8404151518379f9')]
    df_query79fe=df_query[(df_query.devEUI =='a84041c2718379fe')]
    
    tempArr7a0a  =  df_query7a0a.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr7a0a  =  df_query7a0a.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    tempArr7a01  =  df_query7a01.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr7a01  =  df_query7a01.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    tempArr79fd  =  df_query79fd.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr79fd  =  df_query79fd.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    tempArr7a0e  =  df_query7a0e.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr7a0e  =  df_query7a0e.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    tempArr79f9  =  df_query79f9.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr79f9  =  df_query79f9.apply(filterHumfunc, axis = 1).to_numpy().tolist()
    tempArr79fe  =  df_query79fe.apply(filterTempfunc, axis = 1).to_numpy().tolist()
    humArr79fe  =  df_query79fe.apply(filterHumfunc, axis = 1).to_numpy().tolist()

    if(len(humArr7a0a)>0 and len(tempArr7a0a)>0 and len(humArr7a01)>0 and len(tempArr7a01)>0 and len(humArr79fd)>0 and len(tempArr79fd)>0 and len(humArr7a0e)>0 and len(tempArr7a0e)>0 and len(humArr79f9)>0 and len(tempArr79f9)>0 and len(humArr79fe)>0 and len(tempArr79fe)>0 ):
        hourlyTempData7a0a=GetTempHourlyData(tempArr7a0a)
        hourlyHumData7a0a=GetHumHourlyData(humArr7a0a)  
        hourlyTempData7a01=GetTempHourlyData(tempArr7a01)
        hourlyHumData7a01=GetHumHourlyData(humArr7a01)  
        hourlyTempData79fd=GetTempHourlyData(tempArr79fd)
        hourlyHumData79fd=GetHumHourlyData(humArr79fd)  
        hourlyTempData7a0e=GetTempHourlyData(tempArr7a0e)
        hourlyHumData7a0e=GetHumHourlyData(humArr7a0e)  
        hourlyTempData79f9=GetTempHourlyData(tempArr79f9)
        hourlyHumData79f9=GetHumHourlyData(humArr79f9)  
        hourlyTempData79fe=GetTempHourlyData(tempArr79fe)
        hourlyHumData79fe=GetHumHourlyData(humArr79fe)  
    else:
        hourlyTempData7a0a=[]
        hourlyHumData7a0a=[] 
        hourlyTempData7a01=[]
        hourlyHumData7a01=[]  
        hourlyTempData79fd=[]
        hourlyHumData79fd=[]  
        hourlyTempData7a0e=[]
        hourlyHumData7a0e=[]  
        hourlyTempData79f9=[]
        hourlyHumData79f9=[]  
        hourlyTempData79fe=[]
        hourlyHumData79fe=[]  
    return jsonify({"tempData7a0a": hourlyTempData7a0a,"humData7a0a": hourlyHumData7a0a,"tempData7a01": hourlyTempData7a01,"humData7a01": hourlyHumData7a01,"tempData79fd": hourlyTempData79fd,"humData79fd": hourlyHumData79fd,"tempData7a0e": hourlyTempData7a0e,"humData7a0e": hourlyHumData7a0e,"tempData79f9": hourlyTempData79f9,"humData79f9": hourlyHumData79f9,"tempData79fe": hourlyTempData79fe,"humData79fe": hourlyHumData79fe}), 200








if __name__ == '__main__':
    # http_server = WSGIServer(("0.0.0.0", 1240), app)
    # http_server.serve_forever()
    app.run()
