from bson.json_util import dumps
from utilities import *
from constants import *
import time
import os
import avro.schema
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import Queue
import threading
import argparse
import datetime

# Avro Schema description for the change_stream data :

write_schema = avro.schema.parse(json.dumps({
    "namespace": "example.avro",
    "type": "record",
    "name": "change_stream",
    "fields": [
         {"name": "seq_no", "type": "int"},
         {"name": "change_time", "type": "long"},
         {"name": "document_key", "type": ["string", "null"]},
         {"name": "document", "type": ["string", "null"]},
         {"name": "operation_type", "type": ["string", "null"]}

     ]
}))


# Producer process to capture changes on the mongo collection and publish to the change stream Queue
def mongo_listener(q, read_change_cursor):
    try:
        with read_change_cursor as stream:
            for change in stream:
                q.put(change)
                logging.debug("Producer queue size " + str(q.qsize()))

    except:
        logging.info(sys.exc_info())
        e = sys.exc_info()
        raise RuntimeError("Failed to execute mongo_listener %s" % e[ 1 ])


# Consumer process to read from the change stream Queue and write to GCS incoming file location
def record_writer(q):

    # Read the counter file to increment the row sequence counter
    counter = read_counter(counter_file)
    # Timer start
    start = time.time()
    while True:
        try:
            logging.debug(time.time()-start)
            # Wait for certain data size or timeout
            if q.qsize() >= BATCH_SIZE or (time.time()-start > TIMEOUT and not q.empty()):
                logging.info("Queue size : " + str(q.qsize()))
                local_path = LOCAL_DATA_PATH + COLLECTION_TO_WATCH + "/changes_" + COLLECTION_TO_WATCH + ".avro"
                target_path = "gs://"+GCS_BUCKET+"/"+TARGET_GCS_PATH + COLLECTION_TO_WATCH + "/" + "incoming/" + "changes_" + \
                              COLLECTION_TO_WATCH +'_'+time.strftime("%Y%m%d-%H%M%S")+".avro"
                if os.path.isfile(local_path):
                    logging.info("File already exists.")
                    writer = DataFileWriter(open(local_path, "ab"), DatumWriter(), write_schema)
                else:
                    writer = DataFileWriter(open(local_path, "wb"), DatumWriter(), write_schema)

                while not q.empty():

                    counter += 1
                    change_record=q.get()
                    token = change_record.get("_id")
                    if change_record['operationType'] == 'delete':
                        change_time = int(round(time.time() * 1000))
                        writer.append({"seq_no": counter,"change_time": change_time, "document_key": dumps(change_record['documentKey']['_id']),\
                                       "document": '',
                                       "operation_type": change_record['operationType']
                                       })
                    else:
                        change_time = int(round(time.time() * 1000))
                        writer.append({"seq_no": counter,"change_time": change_time, "document_key": dumps(change_record['documentKey']['_id']),
                                       "document": dumps(change_record['fullDocument']),
                                       "operation_type": change_record['operationType']
                                       })
                # Timer reset
                start = time.time()
                writer.close()
                file_move_gcs(LOCAL_DATA_PATH+COLLECTION_TO_WATCH+'/',target_path)
                token_writer(token,counter,token_file,counter_file)
                logging.info("Token written & counter written")
            else:
                logging.info("Sleeping for "+str(TIMEOUT) + "secs : ConsumerThread")
                sleep(TIMEOUT)

        except:
            logging.info(sys.exc_info())
            e= sys.exc_info()
            raise RuntimeError("Failed to execute record_writer %s" % e[ 1 ])


def process_working_files(working_path,archive_path):
    try:

        logging.info("Loading change_stream table")
        load_to_bq(working_path, DATASET + '.' + COLLECTION_TO_WATCH + '_change_stream')
        logging.info("Running Merge Query")
        merge_query(COLLECTION_TO_WATCH + '_change_stream', COLLECTION_TO_WATCH)
        logging.info("Moving files to archive")
        file_move_gcs(working_path, archive_path)
        logging.info("Loading archive table")
        load_to_bq_partitioned(archive_path, DATASET + '.' + COLLECTION_TO_WATCH + '_change_stream_archive',
                               '$' + datetime.datetime.today().strftime('%Y%m%d'))
        logging.info("Removing archive files")
        remove_files(archive_path)
    except:
        logging.info(sys.exc_info())
        e = sys.exc_info()
        raise RuntimeError("Failed to execute record_writer %s" % e[ 1 ])


# All BQ operations are done here. It wakes up every BQ_OPS_TIMEOUT.
# Moves file from incoming to working & loads the data to collection_name_change_stream table
# Merges the data with final_table & overwrites the final data to collection_name table
# Archives the data in collection_name_change_stream_archive table partitioned by day


def bq_operations():
    while True:
        try:

            logging.info("Sleeping for "+str(BQ_OPS_TIMEOUT)+" secs:BQ operations")
            sleep(BQ_OPS_TIMEOUT)
            source_file_path = TARGET_GCS_PATH+COLLECTION_TO_WATCH+"/incoming/"
            working_file_path = TARGET_GCS_PATH + COLLECTION_TO_WATCH + "/working/"
            source_path = "gs://"+GCS_BUCKET+"/"+TARGET_GCS_PATH+COLLECTION_TO_WATCH+"/incoming/"
            working_path = "gs://"+GCS_BUCKET+"/"+TARGET_GCS_PATH+COLLECTION_TO_WATCH+"/working/"
            archive_path = "gs://"+GCS_BUCKET+"/"+TARGET_GCS_PATH + COLLECTION_TO_WATCH  + "/archive/"

            if check_if_files_exist(GCS_BUCKET,source_file_path):
                logging.info("Incoming files are available")
                logging.info("Moving the files to working")
                file_move_gcs(source_path, working_path)
                process_working_files(working_path,archive_path)
            elif check_if_files_exist(GCS_BUCKET,working_file_path):
                logging.info("Working files are present")
                process_working_files(working_path, archive_path)
            else:
                logging.info("No incoming files")

        except:
            e=sys.exc_info()
            raise RuntimeError("BQ operations failed %s" %e[1])
            sys.exit(e[1])


def start_threads(changes):
    try:

        t_producer = threading.Thread(name="ProducerThread", target=mongo_listener, args=(change_stream_queue, changes))
        logging.info("Starting ProducerThread")
        t_producer.start()
        t_consumer = threading.Thread(name="ConsumerThread", target=record_writer, args=(change_stream_queue,))
        logging.info("Starting ConsumerThread")
        t_consumer.start()
        t_bq = threading.Thread(name="BigQueryThread", target=bq_operations, )
        logging.info("Starting BigQueryThread")
        t_bq.start()
    except:
        e = sys.exc_info()
        raise RuntimeError("Failed at starting the threads %s" % e[1])
        sys.exit(e[1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Mongo ingestion to BigQuery")
    parser.add_argument("--collection", help="Collection name you want to configure to load to BQ", type=str)
    parser.add_argument("--mongo_connection", help="Connection name you want to configure to load to BQ", type=str)
    parser.add_argument("--mongo_db", help="Mongo DB name you want to configure to load to BQ", type=str)

    args = parser.parse_args()
    COLLECTION_TO_WATCH = args.collection
    MONGO_CONNECTION = args.mongo_connection
    MONGO_DATABASE = args.mongo_db
    client = mongo_connect(MONGO_CONNECTION,PORT)
    db = client[MONGO_DATABASE]
    token_file = LOCAL_DATA_PATH+COLLECTION_TO_WATCH+'_token.txt'
    counter_file = LOCAL_DATA_PATH+COLLECTION_TO_WATCH+'_counter.txt'
    logging.basicConfig(filename='mongo_listener_'+COLLECTION_TO_WATCH+'.log', filemode='w', format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    change_stream_queue = Queue.Queue(maxsize=2*BATCH_SIZE)
    # Check if Token file is present
    if os.path.getsize(token_file) > 0:
        with open(token_file, 'r') as fr:
            resume_token = pickle.load(fr)
            change_cursor = db[COLLECTION_TO_WATCH].watch(full_document='updateLookup', \
                                                        resume_after=resume_token,max_await_time_ms=600)
            start_threads(change_cursor)
    else:
        change_cursor = db[COLLECTION_TO_WATCH].watch(full_document='updateLookup',max_await_time_ms=600)
        start_threads(change_cursor)



