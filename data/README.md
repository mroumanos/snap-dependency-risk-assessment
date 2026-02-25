# `data/` EFO CSV Dumps

This folder contains CSV exports of EFO records used for analysis and QA.

Current files:
- `efos.latest.csv` (latest snapshot)
- `efos.v1.csv` (versioned copy; currently same schema)

## Schema (`efos.latest.csv`)

Column count: `36`

1. `id`: Row identifier and unique UUID.
2. `created_time`: Timestamp provided from source and if not available, modified_time.
3. `modified_time`: Time pulled from source.
4. `uploaded_time`: Time this row was uploaded/ingested. Usually the same as modified_time.
5. `organization_id`: Source organization ID. This is the ID used by Feeding America.
6. `name`: Location's name. Extracted from the source and cleansed and capitalized.
7. `source_url`: Source endpoint/page URL the data was extracted from.
8. `reference_id`: Source-native record identifier.
9. `type`: List of type/category tags identified through a bag-of-words labeling.
10. `latitude`: Latitude coordinate.
11. `longitude`: Longitude coordinate.
12. `address_full`: Full formatted address.
13. `address_number`: Street number.
14. `street_name`: Street name.
15. `street_name_post_type`: Street suffix (e.g., `ST`, `RD`, `AVE`).
16. `occupancy_type`: Unit type (e.g., `APT`, `STE`) when present.
17. `occupancy_identifier`: Unit/suite number when present.
18. `city_name`: City.
19. `state_code_orig`: Original state code from source data. Column state_code should be used instead.
20. `country_code`: Country code. Currently, only `US`.
21. `zip_code`: Postal code.
22. `phone_number`: Phone number.
23. `metadata`: Raw source payload and enrichment metadata (JSON/text). If it was a deduplicated point, this contains all of the raw metadata from all points, combined into an array with a cluster size included.
24. `duplicate`: Duplicate flag or marker. All should be False.
25. `blkgrp_name`: Census block group name.
26. `tract_name`: Census tract name.
27. `county_name`: County name.
28. `state_name`: State name.
29. `state_code`: Normalized state code.
30. `blkgrp_id`: Census block group ID.
31. `tract_id`: Census tract ID.
32. `county_id`: County FIPS/ID.
33. `state_id`: State FIPS/ID.
34. `division_id`: Census division ID.
35. `region_id`: Census region ID.
36. `geometry`: Geometry value (spatial representation).

## Contact

For questions about this dataset, contact: `mroumanos@pm.me`
