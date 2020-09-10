import numpy
import sqlalchemy
import time
from pyspark.sql import SparkSession

west = -86.9
east = -87.0
north = 32.19
south = 32.20

lats = numpy.random.normal(
    loc=numpy.mean([east, west]),
    scale=numpy.std([east, west]),
    size=100
)
longs = numpy.random.normal(
    loc=numpy.mean([north, south]),
    scale=numpy.std([north, south]),
    size=100
)

places = list(zip(
    [float(x) for x in lats], [float(x) for x in longs]
))


def query_nom(coords):
    db_con = sqlalchemy.create_engine(
        '''postgresql://postgres:dsnominstance@18.234./nominatim''')

    query = '''
        WITH point_details AS (
            SELECT st_line_locate_point(linegeo, st_setsrid('POINT(-86.93944 32.19241)'::GEOMETRY, 4326)) AS fraction,
                st_distance(st_setsrid('POINT(-86.93944 32.19241)'::GEOMETRY, 4326), linegeo) AS distance,
                startnumber,
                endnumber
            FROM location_property_tiger
            WHERE st_dwithin(st_setsrid('POINT(-86.93944 32.19241)'::GEOMETRY, 4326), linegeo, 0.001)
            ORDER BY distance
            LIMIT 1
        )
        SELECT ROUND((endnumber - startnumber) * fraction) AS housenumber
        FROM point_details
        ;
    '''.format(str(coords).replace(',', '').replace('(', '').replace(')', ''))

    results = db_con.execute(query).fetchall()

    return results


spk_sn = SparkSession.builder.master('local').appName('local_demo').getOrCreate()
# df = spk_sn.createDataFrame(douglas_places, schema=['lat', 'long'])
start = time.time()
rdd_list = spk_sn.sparkContext.parallelize(places, 10)
addresses = rdd_list.map(query_nom)
print(rdd_list.take(5))
print(addresses.take(100))

end = time.time()
print(end - start)

