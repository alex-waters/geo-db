# A somewhat generalised script to import .pbf OSM data into a PostGIS database
# The host name, defined under the -H option, obviously needs to be changed to your host
# The options --number-processes and -C can be changed to reflect the hardware resources you've given to osm2pgsql
osm2pgsql -S ds-team.style -lsc -O pgsql --multi-geometry --number-processes 8 -C 58000 --prefix raw_usa -P 5432 -U postgres -H address-finding.cycxgzzqnbk6.us-east-1.rds.amazonaws.com -d spaces us-latest.osm.pbf