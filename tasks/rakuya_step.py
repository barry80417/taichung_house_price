from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd

def extract(**kwargs):
    hook = MongoHook(conn_id="mongo_default")
    collections = hook.get_collection("rakuya", "airflow")
    documents = list(collections.find())
    for doc in documents:
        doc["_id"] = str(doc["_id"])
    hook.close_conn()
    return documents

def transform(**kwargs):
    documents = kwargs.get("ti").xcom_pull(task_ids="t1")
    data1 = []
    for doc in documents:
        data1.append({"建案座落位置" : doc['建案座落位置'], 
                     "建物" : doc['建物'],
                     "主建物" : doc['主建物'],
                     "公共設施" : doc['公共設施'],
                     "土地登記" : doc['土地登記'],
                     "房間" : doc['房間'],
                     "廳堂": doc['廳堂'],
                     "衛浴" : doc['衛浴'],
                     "年份" : doc['年份'],
                     "樓" :doc['樓層'],
                     "車位" : doc['車位與否'],
                     "車位類型" :doc['車位類型'],
                     "開價" : doc['開價']
                     })

    data = pd.DataFrame(data1)
    data['建案座落位置']=data['建案座落位置'].str.split('/').str.get(0)
    data['行政區'] = data['建案座落位置'].str[3:].str.split('區').str.get(0)+'區'
    data['建物'] = data['建物'].str[:-1]
    data['主建物']=data['主建物'].str[:-1]
    data['公共設施']=data['公共設施'].str[:-1]
    data['土地登記']=data['土地登記'].str[:-1]
    data['樓層'] = data['樓'].str.split('/').str.get(0)
    data['總樓層'] = data['樓'].str.split('/').str.get(1)
    data['年份']=data['年份']

    data.drop_duplicates(subset=['建案座落位置','建物','主建物'])
    data=data.to_dict('records')

    return data

def load(**kwargs):
    data = kwargs.get("ti").xcom_pull(task_ids=kwargs.get("t2"))
    if not data:
        return data
    hook = MongoHook(conn_id="mongo_default")
    collections = hook.get_collection("rakuya_detail", "result")
    collections.insert_many(data)
    doc = collections.aggregate([
        {
            '$group':{'_id':{'建案座落位置': '$建案座落位置', '建物': '$建物', '主建物':'$主建物','土地登記': '$土地登記','年份': '$年份'},'count': {'$sum': 1}, 'dups': {'$addToSet': '$_id'}}
        },
        {
            '$match': {'count': {'$gt':1}}
        }])

    for i in doc:
        first = 1
        for j in collections.find(i['_id']):
            if first ==1:
                first=0
                continue
            else:
                collections.delete_one({'_id':j['_id']})
    hook.close_conn()
    return 'done'