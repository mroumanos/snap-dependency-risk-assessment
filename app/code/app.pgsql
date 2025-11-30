CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS clusters (
    cluster_id BIGSERIAL PRIMARY KEY,
    centroid geometry(Point, 4326) NOT NULL
);

ALTER TABLE county_agg
    ADD COLUMN IF NOT EXISTS fa_high_income_threshold DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_low_income_threshold DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_high_income_pct DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_between_income_pct DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_low_income_pct DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_low_income_threshold_programs TEXT,
    ADD COLUMN IF NOT EXISTS fa_high_income_threshold_programs TEXT,
    ADD COLUMN IF NOT EXISTS fa_pop NUMERIC,
    ADD COLUMN IF NOT EXISTS fa_fi_rate DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_fi_pop NUMERIC,
    ADD COLUMN IF NOT EXISTS fa_child_fi_rate DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_child_fi_pop NUMERIC,
    ADD COLUMN IF NOT EXISTS fa_child_fi_below_pct DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_child_fi_above_pct DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_cpm DECIMAL,
    ADD COLUMN IF NOT EXISTS fa_annual_gap_funding_needed NUMERIC,
    ADD COLUMN IF NOT EXISTS fa_associated_bank_ids TEXT[];


UPDATE county_agg c
SET
    fa_high_income_threshold = (x.finding->>'HI_TH')::DECIMAL,
    fa_low_income_threshold = (x.finding->>'LOW_TH')::DECIMAL,
    fa_high_income_pct = (x.finding->>'HI_PCT')::DECIMAL,
    fa_between_income_pct = (x.finding->>'BTWN_PCT')::DECIMAL,
    fa_low_income_pct = (x.finding->>'LOW_PCT')::DECIMAL,
    fa_low_income_threshold_programs = x.finding->>'LOW_TH_PROGS',
    fa_high_income_threshold_programs = x.finding->>'HI_TH_PROGS',
    fa_pop = (x.finding->>'COUNTY_POP')::NUMERIC,
    fa_fi_rate = (x.finding->>'COUNTY_FI_RATE')::DECIMAL,
    fa_fi_pop = (x.finding->>'COUNTY_POP_FI')::NUMERIC,
    fa_child_fi_rate = (x.finding->>'CHILD_FI_PCT')::DECIMAL,
    fa_child_fi_pop = (x.finding->>'CHILD_FI_COUNT')::NUMERIC,
    fa_child_fi_below_pct = (x.finding->>'CHILD_FI_BELOW_PCT')::DECIMAL,
    fa_child_fi_above_pct = (x.finding->>'CHILD_FI_ABOVE_PCT')::DECIMAL,
    fa_cpm = (x.finding->>'COST_PER_MEAL')::DECIMAL,
    fa_annual_gap_funding_needed = (x.finding->>'WT_ANNUAL_DOLLARS')::NUMERIC,
    fa_associated_bank_ids = x.fa_associated_bank_ids
FROM (
    WITH parsed_capacity AS (
        SELECT
            "EntityID" as id,
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE("ListFipsCounty", ' ', ''),
                                        ''',','",'
                                    ),
                                    ''':','":'
                                ),
                                ':''',
                                ':"'
                            ),
                            ',''',
                            ',"'
                        ),
                        '{''',
                        '{"'
                    ),
                    '''}',
                    '"}'
                ) as cleaned_json_text
        FROM feeding_america_foodbanks_raw
    ),
    county_arrays as (
        select
            id,
            cleaned_json_text::jsonb -> 'LocalFindings' AS local_findings
        from parsed_capacity
    )
    select
        elem->>'FipsCode' as county_id,
        elem AS finding,
        array_agg(id) as fa_associated_bank_ids
    from
    county_arrays
    cross join lateral (
        select jsonb_array_elements(
            case
                when local_findings is null then '[]'::jsonb
                when jsonb_typeof(local_findings) = 'array' then local_findings
                else jsonb_build_array(local_findings)
            end
        ) as elem
    )
    group by 1,2
) x
WHERE c.id = x.county_id;


select count(case when fa_high_income_threshold is not null then true else null end) as ct_join, count(1) from county_agg



select * from county_agg limit 10


select * from counties where name = upper('ALAMEDA')



select
    *
FRom food_posts f
inner join counties c
    on c.id = '06001'
    and st_contains(c.geometry, f.geometry) 




SELECT column_name,
    col_description('public.acs_2023_county_raw'::regclass, ordinal_position) AS comment
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'acs_2023_county_raw';

drop table "2023_acs_survey_poverty_county_raw";
drop table "2023_acs_survey_state_raw";
drop table "2023_acs_survey_blk_grp_raw";
drop table "2023_acs_county_raw";
drop table "2023_acs_survey_tract_raw";
drop table "2023_acs_survey_county_raw";
drop table "2024_census_blocks_raw";
drop table "2024_census_counties_raw";
drop table "2024_census_states_raw";
drop table "2024_census_tracts_raw";

select * from acs_2023_county_raw limit 10


CREATE INDEX states_geom_idx ON states USING GIST (geometry);
CREATE INDEX counties_geom_idx ON counties USING GIST (geometry);
CREATE INDEX tracts_geom_idx ON tracts USING GIST (geometry);
CREATE INDEX blkgrps_geom_idx ON blkgrps USING GIST (geometry);
CREATE INDEX food_posts_geom_idx ON food_outposts USING GIST (geometry);
CREATE INDEX snap_retailers_geom_idx ON snap_retailers USING GIST (geometry);



with nn as (
    select
        a.id as a_id,
        b.id as b_id,
        a.name as a_name,
        b.name as b_name,
        a.address as a_address,
        b.address as b_address,
        a.phone_number_1 as a_pn_1,
        b.phone_number_1 as b_pn_1,
        a.phone_number_2 as a_pn_2,
        b.phone_number_2 as b_pn_2,
        a.source as a_source,
        b.source as b_source,
        st_distance(a.geometry::geography, b.geometry::geography) as dist
    from food_outposts a
    cross join lateral (
        select
            id,
            name,
            address,
            phone_number_1,
            phone_number_2,
            source,
            geometry
        from food_outposts f
        where f.id <> a.id
        order by f.geometry <-> a.geometry
        limit 5
    ) b
),
clusters as (
    SELECT
        a_id,
        ARRAY(SELECT element FROM unnest(array_cat(array[a_id], array_agg(b_id))) AS element ORDER BY element) as cluster_id
    FROM
        nn
    WHERE
        (
            dist < 10
            AND a_name = b_name
        )
        OR
        (
            dist < 10
            AND a_address = b_address
        )
        OR
        (
            dist < 10
            AND (
                translate(a_pn_1, '-() ', '') = translate(b_pn_1, '-() ', '')
                OR
                translate(a_pn_1, '-() ', '') = translate(b_pn_2, '-() ', '')
            )
        )
    GROUP BY 1
)
select
    cardinality(cluster_id),
    count(distinct cluster_id)
from clusters
group by 1

-- View exposing only non-duplicate food outposts.
CREATE OR REPLACE VIEW food_outposts_deduped AS
SELECT *
FROM food_outposts
WHERE NOT COALESCE(duplicate, FALSE);
