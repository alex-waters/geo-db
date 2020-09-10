# Before running this, you need to have downloaded the Nominatim preprocessed Tiger data.
# If using a linux machine, this is done with wget and then tar to decompress the files.
# Commands for this should be in the readme of this repo.

# NOTE - This script is written for a Databricks driver and mounted S3 data.

import os
import sqlalchemy
import pickle

# get the file paths for each .sql file
mnt_files = os.listdir('/PATH/TO/MOUNTED/TIGER/SQL/FILES')
# this just corrects the file paths so that Python can find the files
# remove or edit if not using mounted S3
mnt_file_paths = ['/PATH/TO/MOUNTED/TIGER/SQL/FILES/'+x for x in mnt_files]


def data_writer(file_path):
    log = {'file': [], 'written': []}
    db_con = sqlalchemy.create_engine(
        '''postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/tiger-import''',
        connect_args={
                'user': '',
                'password': ''
            }
    )

    # the splitting of files causes some ineffieciency because each SELECT is handled by itself
    # however the alternative is that any data that throws an error will cause the whole transaction to roll back
    # so altough the split increases compute time, it reduces loss of data  
    tiger_lines = open(file_path).read().split(';')
    tiger_queries = [x+';' for x in tiger_lines]

    for q in tiger_queries:
        try:
            db_con.execute(
                sqlalchemy.text(q).execution_options(autocommit=True))
            log['file'].append(file_path)
            log['written'].append(True)
        except:                          # ideally this would be the postgres DataError but it doesn't work (?)
            print('data error')
            log['file'].append(file_path)
            log['written'].append(False)

    return log


distrib_tiger_paths = spark.sparkContext.parallelize(mnt_file_paths, 800)
spark_execute = distrib_tiger_paths.map(data_writer)
results = spark_execute.collect()
# this writes out the logs that were created by the data_writer function
pickle.dump(results, open('/dbfs/mnt/s3/AlexW/tiger_spark_write.p', 'wb'))
# mini check to see if anything has gone drastically wrong with the operation.
# the below numbers should be very close to each other or match
print(
  len(results[0]['file']),
  len([x for x in results[0]['written'] if x])
)
