import requests
import os
import json

import datetime as dt
from dotenv import load_dotenv

import pandas as pd
import pymongo as mongo
from google.cloud import storage

def call_311(date: str, column_dict: dict) -> dict:
    '''
    date: string formatted YYYY-MM-DD
    column_dict: dictionary connecting API json columns and historic data columns

    rename and reformat the API json to one for upload to mongoDB
    '''

    url = f"https://data.sfgov.org/resource/vw6y-z8j6.json?$where=updated_datetime>='{date}'&$limit=999999"
    response = requests.get(url)
    collection = response.json()
    df = pd.DataFrame(collection)
    df = df.rename(columns=column_dict)
    documents = df.to_json(orient='records')
    documents = json.loads(documents)

    return documents 

def get_mongo_collection():
    '''
    use .env to get working collection for mongo database
    '''
    
    mong_uri = os.environ.get("MONGODB_URI")
    mongo_db = os.environ.get("MONGODB_DATABASE")
    mongo_collection = os.environ.get("MONGODB_COLLECTION")

    client = mongo.MongoClient(mong_uri)
    db = client[str(mongo_db)]
    collection = db[str(mongo_collection)]
    
    return collection

def update_database(collection: mongo.collection, documents: dict) -> list:
    '''
    collection: configured mongodb collection cursor/object
    documents: formatted dictionary of new documents to update/insert to the database

    prints final number of total documents matched, updated, and inserted to the collection
    returns a list of upserted _ids for reference if desired
    '''
    
    docs_matched = 0
    docs_updated = 0
    docs_upserted = 0
    upserted_ids = []

    print(f'{len(documents)} documents to update or upload to databse')
    for case in documents:
        for key, item in case.items():
            if key == 'CaseID':
                find = {'CaseID':int(item)}
                result = collection.replace_one(find, case, upsert=True)
                docs_matched += result.matched_count
                docs_updated += result.modified_count
                if result.upserted_id:
                    docs_upserted += 1
                    upserted_ids.append(result.upserted_id)
                if docs_matched or docs_updated or docs_upserted % 100:
                    print(f'existing documents matched: {docs_matched}\n'
                          f'existing documents updated: {docs_updated}\n'
                          f'new documents created: {docs_upserted}')
                    print('Continuing update!')

    print(f'Update complete!')
    print(f'existing documents matched: {docs_matched}\n'
          f'existing documents updated: {docs_updated}\n'
          f'new documents created: {docs_upserted}')
    return upserted_ids

api_to_historic_dict = {'service_request_id': 'CaseID',
                        'requested_datetime': 'Opened',
                        'closed_date': 'Closed',
                        'updated_datetime': 'Updated',
                        'status_description': 'Status',
                        'status_notes': 'Status Notes',
                        'agency_responsible': 'Responsible Agency',
                        'service_name': 'Category',
                        'service_subtype': 'Request Type',
                        'service_details': 'Request Details',
                        'address': 'Address', 'street': 'Street',
                        'supervisor_district': 'Supervisor District',
                        'neighborhoods_sffind_boundaries': 'Neighborhood',
                        'analysis_neighborhood': 'Analysis Neighborhood',
                        'police_district': 'Police District',
                        'lat': 'Latitude', 'long': 'Longitude',
                        'point': 'Point', 'point_geom': 'point_geom',
                        'source': 'Source',
                        'data_as_of': 'data_as_of',
                        'data_loaded_at': 'data_loaded_at',
                        'media_url': 'Media URL',
                        'BOS_2012': 'BOS_2012'}

def update_gcs(bucket_name: str, service_acct: str):
    '''
    update GCS with a file to indicate everything completed 
    in order to activate the AirFlow DAG
    '''
    
    client = storage.Client.from_service_account_json(service_acct)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob('update_complete.txt')
    blob.upload_from_string('update completed! go DAG go!')
    print('GCS bucket updated')


if __name__ == '__main__':
    
    load_dotenv()
    collection = get_mongo_collection()
    print(f'Connected: {collection}')

    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    service_acct = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    date = str(dt.date.today() - dt.timedelta(days= 2))

    print(f'Beginning new document collection')
    new_documents = call_311(date, api_to_historic_dict)
    print(f'new documents collected')

    print(f'Beginning to update database (this will take some time)')
    update_database(collection, new_documents)

    update_gcs(bucket_name, service_acct)