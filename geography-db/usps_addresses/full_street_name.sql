/*
 The USPS has a defined way of splitting and abbreviating addresses.
 This format does not match well with OSM though which mostly uses common
 recognised names input by users.
 To transform USPS addresses into something closer to OSM, we reverse the
 splitting and abbreviating with the below script.
 The new table is the one used for finding full zip4 addresses later.

 There are two tables used in the LEFT JOINs below and they are created
 manually by uploading the corresponding csv files held in the current
 directory. street_suffix_lookup.csv was taken from the USPS website.
 */

CREATE TABLE zip4_fullname AS (
    SELECT lower(concat(
            dpre.name_direction, ' ',
            z.street_name, ' ',
            s.full_name, ' ',
            d.name_direction
        )) AS full_street_name,
        dpre.name_direction, z.street_name,
        z.zip_code,
        z.plus4_high_number_sector,
        z.plus4_high_number_segment,
        z.address_primary_low_number,
        z.address_primary_high_number,
        z.adress_primary_odd_even_code
    FROM zip4_usps z
    LEFT JOIN direction_lookup d
        ON z.street_post_directional_abbreviation = d.direction
    LEFT JOIN direction_lookup dpre
        ON z.street_pre_directional_abbreviation = dpre.direction
    LEFT JOIN street_suffix_lookup s
        ON z.street_suffix_abbreviation = s.abb
)