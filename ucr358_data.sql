select  YEAR,ETHNICITY,SUM(TOTAL) as TOTAL,
SUM(under_one_year) as under_one_year,
SUM(year_1) as year_1,
SUM(years_2) as years_2,
SUM(years_3) as years_3,
SUM(years_4) as years_4,
SUM(under_5_years) as under_5_years,
SUM(years_5_9) as years_5_9,
SUM(years_10_14) as years_10_14,
SUM(years_15_19) as years_15_19,
SUM(years_20_24) as years_20_24,
SUM(years_25_29) as years_25_29,
SUM(years_30_34) as years_30_34,
SUM(years_35_39) as years_35_39,
SUM(years_40_44) as years_40_44,
SUM(years_45_49) as years_45_49,
SUM(years_50_54) as years_50_54,
SUM(years_55_59) as years_55_59,
SUM(years_60_64) as years_60_64,
SUM(years_65_69) as years_65_69,
SUM(years_70_74) as years_70_74,
SUM(years_75_79) as years_75_79,
SUM(years_80_84) as years_80_84,
SUM(years_85_89) as years_85_89,
SUM(years_90_94) as years_90_94,
SUM(years_95_99) as years_95_99,
SUM(years_100_plus) as years_100_plus,
SUM(Not_stated) as Not_stated

from CANCER_MORTALITY_RAW_DATA  where UCR358_CODE in (
70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,
127,128,129,130,131,132,133,134,135,136,137




)
group by YEAR,ETHNICITY
order by YEAR,ETHNICITY;


-- select   CANCER_TYPE,YEAR,ETHNICITY,EXISTING_TOTAL FROM ORIGINAL_CANCER_MORTALITY_RAW_DATA
-- WHERE (CANCER_TYPE) Like '%Lower GI%'
-- order by YEAR,ETHNICITY;

