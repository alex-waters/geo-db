# Run this after roadname_find.py has fully completed

# NOTE - this file is designed to be run on a Databricks cluster

import sqlalchemy

# read in the data written by roadname_find.py
# set the path to what matches the write location of roadname_find.py of course
road_locs = spark.read.parquet('s3a://datascience-prod-ds-wejo/AlexW/road_names')

# add postcodes to the data, this speeds up the query later
road_locs.createOrReplaceTempView('road_locs')
road_pc = spark.sql('''
    SELECT
        r.device_id,
        r.device_pois_centre,
        r.distance AS distance_road,
        r.geohash,
        r.name,
        r.osm_id,
        pc.postal_code AS adept_postcode
    FROM road_locs r
    LEFT JOIN adept_us_refdata.geohash_us_postal_code pc
    ON r.geohash = pc.geohash
''')


def number_finder(coords):
    add_db_con = 'postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/spaces'
    connection_add = sqlalchemy.create_engine(
        add_db_con,
        connect_args={
            'user': 'postgres',
            'password': ''
        }
    )

    housenumber_query = '''
        -- return the interpolated housenumber from tiger
        WITH point_details AS (
        SELECT ST_LineLocatePoint(linegeo, st_setsrid('{}'::GEOMETRY, 4326)) AS fraction,
            st_distance(
                st_setsrid('{}'::GEOMETRY, 4326),
                linegeo) AS distance,
            startnumber,
            endnumber,
            place_id
        FROM public.location_property_tiger
        WHERE st_dwithin(st_setsrid('{}'::GEOMETRY, 4326), linegeo, 0.01)
          AND postcode = '{}'
        ORDER BY distance
        LIMIT 1
        )
        SELECT ROUND((endnumber - startnumber) * fraction) + startnumber AS housenumber,
               place_id AS tiger_place_id
        FROM point_details
        ;
    '''.format(coords[1], coords[1], coords[1], coords[-1])

    try:
        number = connection_add.execute(housenumber_query).fetchall()[0]
        details = {
            'device_id': coords[0],
            'device_pois_centre': coords[1],
            'house_number': int(number[0]),
            'postcode': coords[-1],
            'distance_road': coords[2],
            'geohash': coords[3],
            'name': coords[4],
            'osm_id': coords[5],
            'tiger_place_id': int(number[1])
        }
    except IndexError:
        details = {
            'device_id': coords[0],
            'device_pois_centre': coords[1],
            'postcode': coords[-1],
            'distance_road': coords[2],
            'geohash': coords[3],
            'name': coords[4],
            'osm_id': coords[5]
        }

    return details


road_pc = road_pc.repartition(120)
results = road_pc.rdd.map(number_finder)

results_df = spark.createDataFrame(results)

# write the results to the database
results_df.write.jdbc(
  url='jdbc:postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/spaces',
  table='public.osm_tiger_adds',
  properties={
      'user': 'postgres',
      'password': ''
  }
)
