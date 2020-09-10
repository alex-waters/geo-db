/*
 The query below exists for data governance rather than repeatability.
 It retrieved inferred home locations from a table that already existed
 and added geohashes onto the location data. This was done to speed up the
 subsequent geocoding queries.

 In future, the source of geohashed, WKT home locations can change to
 whatever is considered the best source at that time.
 */

 CREATE TABLE public.home_geohashes AS (
     SELECT device_id,
         st_geohash(centre, 6) AS geohash,
         st_astext(centre) AS wkt_centre,
         st_x(centre) AS longitude,
         st_y(centre) AS latitude
     FROM datascience_test.public.device_pois
     WHERE rank = 1
 );