import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, substring
from pyspark.sql.functions import current_timestamp

def post_to_api():

    spark = SparkSession.builder.appName("S3ToAPI").getOrCreate()

    s3_path = "s3a://journey/event_load"
    df = spark.read.parquet(s3_path)


    def null_filter(jsonrow):
        evprops = jsonrow['property']
        jsonrow_fil = {k:v for k,v in jsonrow.items() if v is not None and v!= '(not set)'}
        ev_fil = {k:v for k,v in evprops.items() if v is not None and v!= '(not set)'}
        jsonrow_fil['props'] = ev_fil

    nonnulldf = df.rdd.map(lambda  x:null_filter((x.asDict(True))))

    def chunks(json_list, apilimit):
        for index in range(0, len(json_list), apilimit)
            yield json_list[index: index + apilimit]
    def process_partition(it):
        jsonlist = []
        exception_from_429 = []
        for row in it:
            jsonlist.append(row)
        for batch in chunks(jsonlist, int(1000)):
            row_list = {'api_key': "username", 'options', 'events': batch}
            payload_size = sys.getsizeof(row_list)
            events_sent = len(row_list['events'])
            session = retry_insertion()
            try:
                session_response = session.post("url", data=gzip.compress(json.dumps(row_list).encode('utf-8')))
            except Exception as e:
                exception_from_429 = [e]
            finally:
                failed_batch = []
                if session_response.status_code != 200:
                    failed_batch = batch

                yield  {'request_id' : session_response.headers['id'],
                        'result': session_response.json(),
                        'status_code': session_response.status_code
                        }

    responserdd = nonnulldf.repartition(2000).mapPartitions(process_partition)



    # Define API endpoint
    api_url = "https://eventexternalapi-endpoint.com"


