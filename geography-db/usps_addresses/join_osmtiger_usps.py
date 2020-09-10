'''
This returns a full USPS zip4 address for all devices.
Some devices will be matched to more than one zip4 code, this is probably because
they are being matched to odd and even sides of the road.
This can be improved by separately joining with the 'odd', 'even', 'both' zip codes
using the USPS field adress_primary_odd_even_code. However the reason not to do this
is because we can't really know which side of the road a device matches best to.
The hope is that even if a device ends up with more than one zip4, they'll be
close enough so as not to harm analysis done using those zip codes.

The joining relies on fuzzy string matching, specifically Levenshtein distance.
The approach is intended as a first pass and there are some obvious improvements
that should be made in future versions.
'''

from pyspark.sql import functions

db_con = 'jdbc:postgresql://address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com:5432/vanilla'

# read in the addresses found using Tiger and OSM data
home_wayid = spark.read.jdbc(
    url=db_con,
    table='public.osm_tiger_adds',
    properties={
        'user': 'postgres',
        'password': ''
    }
)
# read in the USPS zip4 data
zip4_fullname = spark.read.jdbc(
    url=db_con,
    table='public.zip4_fullname',
    properties={
        'user': 'postgres',
        'password': ''
    }
)

# do some data typing
# warning - this use Spark's handy but unsafe type conversions.
# some 'numbers' data will actually contain letters and other characters.
# the conversions below will cause that data to become NULL
# This isn't ideal but is less obstructive than Postgres rejecting the conversion entirely.
# TO DO - improve these conversions to not lose data
zip4_fullname = zip4_fullname.withColumn(
    'address_primary_low_number',
    zip4_fullname['address_primary_low_number'].cast('int')
)
zip4_fullname = zip4_fullname.withColumn(
    'address_primary_high_number',
    zip4_fullname['address_primary_high_number'].cast('int')
)
zip4_fullname = zip4_fullname.withColumn(
    'zip_code',
    zip4_fullname['zip_code'].cast('int')
)
zip4_fullname.createOrReplaceTempView('zip4_fullname')

home_wayid = home_wayid.withColumn(
  'postcode', home_wayid['postcode'].cast('int')
)
home_wayid.createOrReplaceTempView('home_osm_tiger')

# do quite a wide join that will return many matches for each device
cte = spark.sql('''
    SELECT DISTINCT
        h.device_id,
        h.osm_id,
        h.postcode,
        h.geohash,
        h.device_pois_centre,
        h.house_number,
        h.roadname,
        z.plus4_high_number_sector,
        z.plus4_high_number_segment,
        z.full_street_name      
    FROM zip4_fullname z
    INNER JOIN home_osm_tiger h
        ON  h.postcode = z.zip_code
        AND h.house_number BETWEEN z.address_primary_low_number AND Z.address_primary_high_number
''')
# using the wide join above, calculate how close the street names are to each other
# the substring clumsily tackles the problem of very long 'ceremonial' type names preventing a good match
# E.g 'Interstate 270' == 'Interstate 270 Dwight D. Eisenhower Memorial Highway Washington National Pike'
cte_lev = cte.withColumn(
    'name_match',
    functions.levenshtein(
        functions.substring(cte['full_street_name'], 1, 15),
        functions.substring(cte['roadname'], 1, 15)
    )
)
cte_lev.createOrReplaceTempView('cte_lev')

# select the best matches from above
# NOTE - there can be more than one road jointly ranked in first place
home_adds = spark.sql('''
    WITH scored AS(
        SELECT
            *,
            RANK() OVER(PARTITION BY device_id ORDER BY name_match ASC) AS score_rank
        FROM cte_lev
    )
    SELECT *
    FROM scored
    WHERE score_rank = 1
''')

# this is an important check to see how many devices have been lost in the INNER JOIN matching
# some devices will have been lost because they've not been matched but this should be a 'reasonable' number
home_adds.cache()
print(home_adds.agg(functions.countDistinct('device_id')).show())

home_adds.write.jdbc(
  url=db_con,
  table='public.zip4_home_adds',
  properties={
      'user': 'postgres',
      'password': ''
  }
)
