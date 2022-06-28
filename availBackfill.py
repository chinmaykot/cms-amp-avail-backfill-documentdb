import sys
import pymongo
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor

def setEnvVariable():
    os.environ['PAGE_SIZE'] = "2"
    os.environ['TOTAL_PAGE'] = "7"
    os.environ['MAX_WORKER'] = "5"
    os.environ['DVA_URL'] = "https://hes-dva-service-prod.video.k8s.hotstar-labs.com/dva/api/v1/mmc/download"

def connectDatabase():
    password = "Xas232dSas4f134"
    client = pymongo.MongoClient(
        'mongodb://cms_sourcing_dashboard_user:' + password + '@cms-sourcing-dashboard-dev-in-ap-south-1.cluster-cn2w2llmy7rq.ap-south-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    return client

def setPackageData(package, info_db_col):
    radarId = package['radar_id']
    contentId = package['hs_content_identifier']
    entityType = package['entity_type']
    title = package['title']
    season_no = package['season_no']
    episode_no = package['episode_no']
    eidrId = package['alid_id']
    alidId = package['alid_id']
    ampStatus = None
    publishDate = package['publish_dt']

    sdData = info_db_col.find_one({'radarId': radarId})
    if(sdData == None):
        sdData = {
            "radarId": radarId,
            "contentId": contentId,
            "entityType": entityType,
            "title": title,
            "episodeNo": episode_no,
            "seasonNo": season_no,
            "ampStatus": ampStatus,
            "packages" : [
                {
                    "alidId": alidId,
                    "eidrId": eidrId,
                    "tenants": getTenants(package),
                    "availStatus": package['package_type'],
                    "avStatus": None,
                    "publishDate": publishDate,
                }
            ]
        }
        info_db_col.insert_one(sdData)
        print("New doc inserted", radarId)

    else:
        isPresent = False
        for p in sdData["packages"]:
            if p["eidrId"] == eidrId or p["alidId"] == alidId:
                p["availStatus"] = package['package_type']
                isPresent = True

        if (isPresent == False):
            d = {
                "alidId": alidId,
                "eidrId": eidrId,
                "tenants": getTenants(package),
                "availStatus": package['package_type'],
                "avStatus": None
            }
            sdData["packages"].append(d)

        info_db_col.update_one({'radarId': radarId}, {"$set": sdData})
        print("doc updated", radarId)

def getTenants(package):
    tenants = []
    for tenant in package['publish_dt']:
        tenants.append(tenant['tenant_id'])
    return tenants

def setAssetData(package,asset_db_col):
    radarId = package['radar_id']
    alid = package['alid']

    for tenantDate in package['publishDate']:
        tenant = tenantDate['tenant_id']

        # radarId matches and tenant is also present in tenants list
        sdData = asset_db_col.find_one({'radarId':radarId},{'alid':alid})

        if(sdData == None):
            sdData = {
                "radarId": package['radar_id'],
                "type": 'avail',
                "alid": package['alid_id'],
                "eidr": package['alid_id'],
                "tenants": getTenants(package),
                "language": None,
                "status": package['package_status'],
                "subtype": None,
                "trackId": None,
                "recieveTime": package['create_time']
            }
            asset_db_col.insert_one(sdData)
            print("New avail asset inserted", radarId, alid)

        else:
            #add deleltion logic here
            asset_db_col.delete_one({'radarId': radarId})
            sdData = {
                "radarId": package['radar_id'],
                "type": 'avail',
                "alid": package['alid_id'],
                "eidr": package['alid_id'],
                "tenants": getTenants(package),
                "language": None,
                "status": package['package_status'],
                "subtype": None,
                "trackId": None,
                "recieveTime": package['create_time']
            }
            asset_db_col.insert_one(sdData)
            print("New artwork asset inserted", radarId, alid)

    return 0

def convertPackageDoc(package,info_db_col,asset_db_col):
    setPackageData(package,info_db_col)
    setAssetData(package,asset_db_col)

def setData(data,info_db_col,asset_db_col):
    data_len = len(data)
    failed = []
    for count in range(data_len):
        try:
            doc = convertPackageDoc(data[count],info_db_col,asset_db_col)
        except:
            failed.append(count)
            continue

def dump(page_number,page_size,info_db_col,asset_db_col):

    start = time.time()

    statement = "Executing page num " + str(page_number)
    cmd = "echo " + statement
    os.system(cmd)

    params = dict(page_size=page_size, page_number=page_number)
    url = "https://hes-dva-service-prod.video.k8s.hotstar-labs.com/dva/api/v1/package"
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"page number: {page_number}")
        raise Exception("Non 200 status from dva server")
    data = resp.json()
    dataList = data['results']['data']

    setData(dataList,info_db_col,asset_db_col)

    # main(dataList,dynamotable)
    statement = "DONE " + str(page_number) + " time " + str(time.time()-start)
    cmd = "echo " + statement
    os.system(cmd)

def main():
    client = connectDatabase()
    db = client.sourcing_dashboard
    info_db_col = db.sourcing_dashboard_data
    asset_db_col = db.sourcing_dashboard_asset_data

    # test_col_1 = db.sourcing_dashboard_data_test
    # test_col_2 = db.sourcing_dashboard_asset_data_test

    # d = getSourcingDashboardData()

    # test_col_1.insert_one(d)
    # test_col_1.update_one({'radarId':'md:cid:org:disney.com:radar:147133'},{"$set":d})
    # x = test_col_1.find({'radarId': 'md:cid:org:disney.com:radar:147133'})
    # for i in x:
    #     print(i)
    # test_col_1.drop()
    # f = info_db_col.find()
    # for i in f:
        # print(i)
    # print(db.list_collection_names())
    page_size = int(os.getenv("PAGE_SIZE"))
    total_page = int(os.getenv("TOTAL_PAGE"))
    worker = int(os.getenv("MAX_WORKER"))

    with ThreadPoolExecutor(max_workers=worker) as exe:
        for i in range(1, total_page):
            exe.submit(dump, i, page_size,info_db_col,asset_db_col)

    return 0

if __name__ == '__main__':
    setEnvVariable()
    # main()