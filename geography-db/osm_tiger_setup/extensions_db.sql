
/*
These extensions were installed. They come from the PostGIS docs.
HStore was added because it's needed for OSM data
*/

CREATE EXTENSION hstore;
-- Enable PostGIS (as of 3.0 contains just geometry/geography)
CREATE EXTENSION postgis;
-- Enable Topology
CREATE EXTENSION postgis_topology;
-- fuzzy matching needed for Tiger
CREATE EXTENSION fuzzystrmatch;
-- rule based standardizer
CREATE EXTENSION address_standardizer;
-- example rule data set
CREATE EXTENSION address_standardizer_data_us;


/*
Only install this after OSM import
*/
-- Enable US Tiger Geocoder
CREATE EXTENSION postgis_tiger_geocoder;


/*
These extensions below are noted in the PostGIS docs but are not
supported by Amazon RDS
 */

-- enable raster support (for 3+)
CREATE EXTENSION postgis_raster;
-- Enable PostGIS Advanced 3D
-- and other geoprocessing algorithms
-- sfcgal not available with all distributions
CREATE EXTENSION postgis_sfcgal;
