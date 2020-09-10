'''
The USPS Zip+4 dataset that we received were zipped fixed width files.
To make it easier to use, the below script transforms the file into a
database table.

Although the below was run on a Databricks driver, it doesn't use Spark
and can be run in a reasonable time from a mid-range laptop.
'''

import sqlalchemy
import pandas as pd
import zipfile
import os

# hold all the file paths in a list
# the paths will need editing to suit your location
file_names = [
    '/dbfs/mnt/s3/AlexW/zip4/' + x for x in os.listdir('/dbfs/mnt/s3/AlexW/zip4/')
]

# the below will attempt to unzip the files and extract the content
content = []
errors = []
for fn in file_names:
    try:
        zipped_content = zipfile.ZipFile(fn)
        for file in zipped_content.infolist():
            str_content = zipped_content.open(file).read()
            line_width = 182
            split_lines = [
                str_content[i:i + line_width] for i in range(0, len(str_content), line_width)]
            content.append(split_lines)
    except zipfile.BadZipFile:
        errors.append(fn)

# write the output from above into a file, change the path to match yours
with open('/dbfs/mnt/s3/AlexW/zip4_lines.txt', 'w') as wr:
    for i in content:
        for j in i:
            wr.write(str(j.decode()) + '\n')
# keep a record of which files couldn't be accessed
with open('/dbfs/mnt/s3/AlexW/errors.txt', 'w') as error_file:
    error_file.write(str(errors))

# these column widths were taken from the data documentation received
columns = {
    'copyright_detail_code': (0, 1),
    'zip_code': (1, 6),
    'update_key_number': (6, 16),
    'action_code': (16, 17),
    'record_type_code': (17, 18),
    'carrier_route_id': (18, 22),
    'street_pre_directional_abbreviation': (22, 24),
    'street_name': (24, 52),
    'street_suffix_abbreviation': (52, 56),
    'street_post_directional_abbreviation': (56, 58),
    'address_primary_low_number': (58, 68),
    'address_primary_high_number': (68, 78),
    'adress_primary_odd_even_code': (78, 79),
    'building_or_firm_name': (79, 119),
    'address_secondary_abbreviation': (119, 123),
    'address_secondary_low_number': (123, 131),
    'address_secondary_high_number': (131, 139),
    'address_secondary_odd_even_code': (139, 140),
    'plus4_low_number_sector': (140, 142),
    'plus4_low_number_segment': (142, 144),
    'plus4_high_number_sector': (144, 146),
    'plus4_high_number_segment': (146, 148),
    'base_alternate_code': (148, 149),
    'lacs_status_indicator': (149, 150),
    'government_building_indicator': (150, 151),
    'finance_number': (151, 157),
    'state_abbreviation': (157, 159),
    'county_number': (159, 162),
    'congressional_district_number': (162, 164),
    'municipality_city_state_key': (164, 170),
    'urbanization_city_state_key': (170, 176),
    'preferred_last_line_city_state_key': (176, 182)
}

dtype = {colname: 'str' for colname in columns.keys()}
# read the extracted content from above into a pandas df
# the chunksize keeps the data manageable and enables easy inserting to Postgres
reader = pd.read_fwf(
    '/dbfs/mnt/s3/AlexW/zip4_lines.txt',
    skiprows=1,
    colspecs=list(columns.values()),
    names=list(columns.keys()),
    dtype=dtype,
    chunksize=1000000
)
# iterate over the file readers and write their data to Postgres
# the print() statement allows you to monitor progress easily
db_con = sqlalchemy.create_engine(
    'postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/spaces',
    connect_args={
        'user': 'postgres',
        'password': ''
    }
)
counter = 0
for r in reader:
    counter += 1
    if counter > 18:
        r.to_sql(
            name='zip4_usps',
            con=db_con,
            if_exists='append',
            method='multi'
        )
        print(counter)
