# This script takes a list of latlong coordinates, distributes them over a Spark cluster using an RDD, requests an address for each coordinate from OSM and Tiger
# Although the SQL is reasonable, it is intended as a demo and may need editing for your use case.
# The road_query SQL especially is sensitive to how the OSM data has been imported. In some cases it may return a railway or some other OSM feature. Use a WHERE clause to control this.
# NOTE - this script is written for a Databricks cluster. If run outside of that, it needs pyspark setting up explicitly.

import sqlalchemy

latlong_coords = ['-122.460046 48.764339', '-115.81193 33.73821']


def address_finder(coords):
    db_con = 'postgresql://ds-test-db.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/datascience_test'
    connection = sqlalchemy.create_engine(
        db_con,
        connect_args={
            'user': '',
            'password': ''
        }
    )
    road_query = '''
        --get address from north_america_roads
        SELECT type,
               name,
                st_distance(
                st_setsrid('POINT({})'::GEOMETRY, 4326),
                st_transform(geometry, 4326)
                ) AS distance
        FROM import.north_america_roads
        WHERE ST_Intersects(
                st_transform(geometry, 4326),
                st_buffer(st_setsrid('POINT({})'::GEOMETRY, 4326), 0.001)
            )
            AND type = 'residential'
        ORDER BY distance
        LIMIT 1;
    '''.format(coords, coords)
    housenumber_query = '''
        -- return the interpolated housenumber from tiger
        WITH point_details AS (
            SELECT ST_LineLocatePoint(linegeo, st_setsrid('POINT({})'::GEOMETRY, 4326)) AS fraction,
                st_distance(st_setsrid('POINT({})'::GEOMETRY, 4326), linegeo) AS distance,
                startnumber,
                endnumber
            FROM import.tiger_housenumbers
            WHERE st_dwithin(st_setsrid('POINT({})'::GEOMETRY, 4326), linegeo, 0.001)
            ORDER BY distance
            LIMIT 1
        )
        SELECT ROUND((endnumber - startnumber) * fraction) + startnumber AS housenumber
        FROM point_details
        ;
    '''.format(coords, coords, coords)

    road = connection.execute(road_query).fetchall()[0]
    number = connection.execute(housenumber_query).fetchall()[0]

    details = {
        'road_type': road[0],
        'road_name': road[1],
        'distance_from_point': road[2],
        'house_number': int(number[0])
    }

    return details


distributed_places = spark.sparkContext.parallelize(latlong_coords)
results = distributed_places.map(address_finder)
print(results.take(1))
