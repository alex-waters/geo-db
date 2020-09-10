# The script here is a Spark job that will find roadname data from OSM for latlong locations
# It retrieves device home locations by selecting from public.home_geohashes
# That source of locations doesn't get refreshed and the best source should be chosen at run
# The provenance of home_geohashes is shown in home_geohashes_create.sql

# NOTE - this file is designed to be run on a Databricks cluster

import sqlalchemy

spaces_con = 'jdbc:postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/spaces'
s3_con = 's3a://datascience-prod-ds-wejo/AlexW/road_names'    # choose a suitable location here

device_locs = spark.read.jdbc(
  url=spaces_con,
  properties={
    'user': 'postgres',
    'password': ''
  },
  table='''
    (SELECT 
      device_id, 
      geohash, 
      wkt_centre
    FROM public.home_geohashes
    ) s
  '''
)


def road_finder(coords):
    add_db_con = 'postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/spaces'
    connection_add = sqlalchemy.create_engine(
        add_db_con,
        connect_args={
            'user': 'postgres',
            'password': ''
        }
    )

    road_query = '''
        SELECT
            osm_id,
            name,
            geohash,
            st_distance(
            st_setsrid('{}'::GEOMETRY, 4326),
            st_transform(way, 4326)
            ) AS distance
        FROM public.roadnames r     -- created by osm_tiger_setup/roadname_build.sql
        WHERE ST_Intersects(
            st_transform(way, 4326),
            st_buffer(st_setsrid('{}'::GEOMETRY, 4326), 0.01)
            )
            AND geohash LIKE '{}'
        ORDER BY distance
        LIMIT 1;
    '''.format(coords[2], coords[2], coords[1])

    try:
        road = connection_add.execute(road_query).fetchall()[0]
        details = {
            'device_id': coords[0],
            'device_pois_centre': coords[2],
            'house_number': int(road[0]),
            'osm_id': int(road[0]),
            'name': road[1],
            'geohash': road[2],
            'distance': road[3]
        }
    except IndexError:
        details = {
            'device_id': coords[0],
            'device_pois_centre': coords[2]
        }

    return details


# careful here - over partitioning the data will cause PostGIS resources to bottleneck
# 120 partitions might not be perfect but it did work
device_locs = device_locs.repartition(120)
results = device_locs.rdd.map(road_finder)

results_df = spark.createDataFrame(results)

results_df.write.parquet(s3_con)
