/*
 This file should be run at the end of database building and setup.

 Although this will duplicate data already on disk, it creates a table that is
 vastly quicker to query than the whole of raw_usa_line.
 The WHERE filter is somewhat arbitrary and should be changed where necessary.

 Expect the st_geohash() function to take some time.
 */

ALTER TABLE raw_usa_line ADD geohash TEXT;
UPDATE raw_usa_line SET geohash = st_geohash(way, 6);

CREATE TABLE roadnames AS (
    SELECT osm_id,
           highway,
           name,
           geohash,
           way
    FROM raw_usa_line
    WHERE highway NOT IN ('primary', 'motorway_link', 'motorway', 'trunk', 'cycleway', 'footway')
);

CREATE INDEX roadnames_geohash_index
	ON roadnames (geohash);

-- add this index to speed up the housenumber_find.py query
CREATE INDEX tiger_pc_ix
    ON location_property_tiger (postcode);