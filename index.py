import requests
import json
import time
import pandas as pd
#import datetime
import warnings
import numpy as np
import copy
import os
from datetime import datetime,timedelta
from scipy import stats
warnings.filterwarnings("ignore")
import platform
version = platform.python_version().split(".")[0]

if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()
base_url = config["api"]["meta"]

bu = os.environ.get("BU")
url = base_url+'/units?filter={"where":{"stackDeploy":true,"name":{"nlike":"test"},"bu":"'+bu+'"}}'
cont = json.loads(requests.get(url).content)
units = {i['id']:i['name'] for i in cont}

def mapLtags(tagslist,tagsdict):
    eqpIds = list(set(tagsdict[i] for i in tagsdict if i in tagslist))
    batch_size=100
    eqpId_batches = [eqpIds[i:i+batch_size] for i in range(0, len(eqpIds), batch_size)]
    tagsmapping = {}
    for eqpId_batch in eqpId_batches:
        eqp_url = base_url+'/equipment?filter={"where":{"id":{"inq":'+json.dumps(eqpId_batch)+'}},"fields":["id","equipmentLoad"]}'
        resp1 = requests.get(eqp_url)
        #print(resp1.content)
        cont1 = json.loads(resp1.content)
        #print(cont1)
        eqpIdsmapping = {i['id']:{"loadTag":i['equipmentLoad']['loadTag'],"bucketSize":i['equipmentLoad']['loadBucketSize']} for i in cont1}
        tagsmapping.update({i: eqpIdsmapping[tagsdict[i]] for i in tagslist if tagsdict[i] in eqpId_batch})

    return tagsmapping

def getValues(tag,url):
    query = {
    "metrics": [
    {
    "tags": {},
    "name": tag,
        
    }
    ],
    "plugins": [],
    "cache_time": 0,
    "start_relative": {
        "value": "2",
        "unit": "months"
    }
    }

    data = requests.post(url,json=query).json()
    #print data
    #exit()
    try:
        results = data['queries'][0]['results']
        df = pd.DataFrame(results[0]['values'],columns=['date',tag])
        #df['date'] = df['time']
        #df.drop("time",inplace=True,axis=1)

        return df
    except:
        return 
    
def fetch_data(dataTagId,unit_id):
    q=dataTagId
    urlQuery = base_url + '/units/' + unitId + '/tagmeta?filter={"where":{"dataTagId":"'+q+'"},"fields":["benchmarkLoad","dataTagId"]}'
    print(urlQuery)
    response = requests.get(urlQuery)
    #print(response.content)
    if response.status_code == 200:
        print(response.status_code)
        allTags = json.loads(response.content)
        benchmarkLoad_df = {}
        #print(allTags)
        for tag in allTags:
            benchmarkLoad_df[tag['dataTagId']] = pd.DataFrame(columns=['bucket'])
            benchmarkLoad = tag.get('benchmarkLoad', {})
            #print(benchmarkLoad)
            if benchmarkLoad:
                for bucket, values in benchmarkLoad.items():
                    if bucket.isdigit():
                        std = values.get('sd')
                        median = values.get('median')
                        startTime = values.get('startTime')
                        
                        # Convert startTime to the desired format
                        if startTime is not None:
                            start_time_formatted = datetime.utcfromtimestamp(startTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            start_time_formatted = None
                        
                        if all(val is not None for val in [median, std, startTime]):
                            sensitivity=1
                            bias = 0
                            upper = median + bias + 4 * std * sensitivity 
                            lower = median + bias - 4 * std * sensitivity

                            
                            benchmarkLoad_df[tag['dataTagId']] = benchmarkLoad_df[tag['dataTagId']].append({'bucket': float(bucket), 'Time': start_time_formatted, 'median': float(median), 'std': float(std), 'oldUpper': float(upper), 'oldLower': float(lower),"bias":bias,"sensitivity":sensitivity}, ignore_index=True)
                            
            else:
                # No benchmarkLoad available for this dataTagId
                print("No benchmarkLoad available for dataTagId:", tag['dataTagId'])
        #display(benchmarkLoad_df)
        return benchmarkLoad_df[dataTagId]
    
def createSublist(df,column):
    sublist = df[["date",column]].values.tolist()
    #sublist = [[row['date'], row[column]] for index, row in df.iterrows()]
    return sublist

def delete_data2(startEpoch,endEpoch,metrics):

    query = {}
    query["metrics"] = []
    query["metrics"].append({"name":metrics})

    # query["start_relative"] = {"value":"3","unit":"years"}
    query["start_absolute"] = startEpoch
    query["end_absolute"] = endEpoch

    # print config["api"]["datapoints"] + "/delete"
    #print(query)
    url =  base_url.replace("/exactapi",'/api/v1/datapoints/delete')
    res = requests.post(url, json=query)
    print((res.status_code))


def postDataApi(outputTag,store_vals_to_post,start_epoch,end_epoch):
    batch_size = 40000
    url =  base_url.replace("/exactapi",'/api/v1/datapoints')
    delete_data2(start_epoch,end_epoch,outputTag)
    for i in range(0, len(store_vals_to_post), batch_size):
        batch = store_vals_to_post[i:i+batch_size]
        #print(batch)
        body = [{
            "name": outputTag,
            "datapoints": batch,
            "tags":{"type":"historic"}}]
        res = requests.post(url = url,json = body,stream=True)
        time.sleep(1)
        print(res.content) if res.status_code!=204 else None
        print(res.status_code)
        #print(res.content)
def get_bucketSize(dataframe):
    differences = dataframe['bucket'].diff().dropna().squeeze()
    mode_result = stats.mode(differences)
    return int(mode_result.mode[0])

for unitId in units:
    url = base_url+'/units/'+unitId+'/tagmeta?filter={"fields":["dataTagId","equipmentId"]}'
    print(url)
    response = requests.get(url)
    print("response " ,response)
    tagmeta=json.loads(response.content)
    tags = {i['dataTagId']:i['equipmentId'] for i in tagmeta}
    url = base_url+'/units/'+unitId+'/incidents?filter={"where":{"startTime":{"gt":"2023-07-21T00:00:00.000Z"}},"fields":["criticalTags"]}'
    print(url)
    response = requests.get(url)
    print("response " ,response)
    incidents=json.loads(response.content)
    tags_ct=[]
    for i in incidents:
        for ct in i['criticalTags']:
            tags_ct.append(ct['dataTagId'])
    ctags = list(set(tags_ct))
    ctags = [i for i in ctags if i in tags]
    csvName = units[unitId]+"incidenttags.csv"
    pd.DataFrame(ctags,columns=["dataTagId"]).to_csv(csvName)
    loadtagsmap = mapLtags(ctags,tags)
    print(len(loadtagsmap))

    data = {}
    kairosUrl = base_url.replace("/exactapi", 'api/v1/datapoints/query')
    #start_epoch = int(datetime(2023,9,5,0,0,0).strftime('%s'))*1000
    #end_epoch = int(datetime(2023,9,21,15,0,0).strftime('%s'))*1000
    x=1
    for j in ctags:
        print(x)
        print(j)
        x+=1
        df = pd.DataFrame()
        dataTagId=j
        dfGroup = fetch_data(dataTagId,unitId)
        loadtag=loadtagsmap[j]['loadTag']
        try:
            bucketSize = int(loadtagsmap[j]['bucketSize'])
        except:
            get_bucketSize(dfGroup)
        
        data1 = getValues(url=kairosUrl, tag=dataTagId)
        data2 = getValues(url=kairosUrl,tag =loadtag)
        
        d = pd.DataFrame(data1)
        #print(d.isna().sum())
        d = d.drop_duplicates(subset='date', keep='first')
        # d = d.set_index('date')
        d.dropna(axis=0,inplace=True)
        df = pd.concat([df, d], axis=1)
        d = pd.DataFrame(data2)
        #print(d.isna().sum())
        d.rename(columns={'date': 'date_lt'}, inplace=True)
        d = d.drop_duplicates(subset='date_lt', keep='first')
        d.dropna(axis=0,inplace=True)
        # d = d.set_index('date')
        df = pd.concat([df, d], axis=1)

        df.fillna(method='ffill',inplace=True)
        df.reset_index(drop=True, inplace=True)
        if dataTagId==loadtag:
            df=df
        else:
            df = df[df[loadtag] > 0]
        df['bucket']=None
        df['bucket'] = df[loadtag] // bucketSize * bucketSize
        df = df.dropna()
        df['loadLw']=None
        df['loadUp']=None
        if len(df)==0:
            continue
        for i , row in dfGroup.iterrows():
            #print("in")
            df.loc[df[df['bucket'] ==row['bucket']].index,"loadUp"] = row['oldUpper']
            df.loc[df[df['bucket'] ==row['bucket']].index,"loadLw"] = row['oldLower']
        df.dropna(inplace=True)
        #print(df)
        try:
            start_epoch = df['date'].iloc[0].astype(float)
            end_epoch = df['date'].iloc[-1].astype(float)
        except Exception as ex:
            print(ex)
            continue
        #print(type(start_epoch),type(end_epoch))
        sublistUp = createSublist(df,"loadUp")
        postDataApi("loadUp_"+dataTagId,sublistUp,start_epoch,end_epoch)
        sublistLw = createSublist(df,"loadLw")
        postDataApi("loadLw_"+dataTagId,sublistLw,start_epoch,end_epoch)


  
