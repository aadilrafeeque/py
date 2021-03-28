import geohash
from collections import defaultdict
from airflow.models import DAGfrom datetime import datetime
from sweeper.model.blue.commerce_store import CommerceStore
from cl.utils.airflow_base.operators import MongoToS3ExtractOperator
from cl.utils.airflow_base.operators import TDLoadOperator
from cl.utils.airflow_base.utilities import AirflowUtilities
from data_platform.airflow_pipeline import POOL, QUEUE, MARKER
from cl.utils.iter import partition#pylint: disable=unused-argument#pylint: disable=W0108

DEBUG_MODE = AirflowUtilities.is_testing_host()
TD_DB = "analytics"
TD_TABLE = "geohash_clustered_fusion_stores" if not DEBUG_MODE else "geohash_clustered_fusion_stores_test"

TD_SCHEMA = [ 
  ("id", "string"),
  ("latitude", "float"), 
  ("longitude", "float"),
  ("geohash", "string"), 
  ("small_cluster", "string"), 
  ("medium_cluster", "string"), 
  ("large_cluster", "string"), 
  ("huge_cluster", "string")]


  EXECUTION_POOL = POOL.TESTING if DEBUG_MODE else POOL.PROD_DATA
  EXECUTION_QUEUE = QUEUE.TESTING if DEBUG_MODE else QUEUE.PROD_DATA
  # Quick Reference For Column Names
  # small_cluster = 1.2 km x 0.6 km regions
  # medium_cluster = 1.2 km x 1.2 km regions
  # large_cluster = 4.9 km x 4.9 km regions
  # huge_cluster = 39 km x 19.5 km regions


  # Algorithm to group together stores with the same hash
  def cluster(hashed, data, label): 
    mapping = {} 
    counter = 1 
    for x in hashed: 
        if hashed[x] in mapping: 
            data[x][label] = mapping[hashed[x]] 
        else: 
            mapping[hashed[x]] = counter 
            data[x][label] = mapping[hashed[x]] 
            counter += 1

def extract_and_cluster(self, **kwargs): 
    _, _ = self, kwargs 
    data = defaultdict(lambda: defaultdict(lambda: None))

    store_iter = CommerceStore.find_iter({}, slave_ok="offline", max_time_ms=0) 
    for store_chunk in partition(store_iter, 500): 
        for store in store_chunk: 
            if store.longitude < 180 and store.longitude > -180 and store.latitude < 90 and store.latitude > -90:
             data[str(store.id)] = {"id": str(store.id), "latitude": store.latitude, "longitude": store.longitude} 
    # Hashing coordinates into different sizes of boxes based on geohash precision 
    hashed_dict = defaultdict(lambda: defaultdict(lambda: None)) 
    for x in data: 
    hashed_dict['small_cluster'][x] = geohash.encode(data[x]['latitude'], data[x]['longitude'], precision=6) 
    hashed_dict['large_cluster'][x] = geohash.encode(data[x]['latitude'], data[x]['longitude'], precision=5) 
    hashed_dict['huge_cluster'][x] = geohash.encode(data[x]['latitude'], data[x]['longitude'], precision=4) 
    # Making 1.2km x 1.2km boxes by grouping two 1.2km x 0.6km boxes using the lower box's hash 
    group_dict = { 'p' : 'n', 'r' : 'q', 'x' : 'w', 'y' : 'z', 'j' : 'h', 'm' : 'k', 't' : 's', 'v' : 'u', '5' : '4', '7' : '6', 'e' : 'd', 'g' : 'f', '1' : '0', '3' : '2', '9' : '8', 'c' : 'b'} 
    
    for x in hashed_dict['small_cluster']: 
    if hashed_dict['small_cluster'][x][-1] in group_dict: hashed_dict['medium_cluster'][x] = hashed_dict['small_cluster'][x][0:-1] + group_dict[hashed_dict['small_cluster'][x][-1]] 
    else: hashed_dict['medium_cluster'][x] = hashed_dict['small_cluster'][x] 

    # Adding geohash to eventual to_load task 
    for x in data: data[x]['geohash'] = hashed_dict['small_cluster'][x] 

    # Clustering based on the hashes and adding to eventual to_load task 
    for label in hashed_dict: 
        cluster(hashed_dict[label], data, label) 
    
    to_load = [] 

    for x in data: 
        to_load.append(data[x]) 

    return to_load


    default_args = AirflowUtilities.update_default_args(  
        start_date=datetime(2020, 5, 29),
        queue=EXECUTION_QUEUE, 
        pool=EXECUTION_POOL,)


    dag = DAG( dag_id="ClusteringFusionStoresV1", 
        default_args=default_args, schedule_interval="0 7 * * *", 
        catchup=False)


    extract_task = MongoToS3ExtractOperator( task_id="ExtractAndClusterTask", 
        dag=dag, 
        target="geohash_clustering_extract", 
        extract=extract_and_cluster, 
        marker=MARKER, 
        depends_on_past=False,)

        load_task = TDLoadOperator( 
            task_id="LoadClusterTask", 
            dag=dag, 
            schema=TD_SCHEMA, 
            marker=MARKER, 
            td_db=TD_DB, 
            target=TD_TABLE, 
            truncate=True, 
            dependencies=[extract_task], 
            depends_on_past=False,)