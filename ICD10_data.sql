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

from ICD10_CANCER_MORTALITY_RAW_DATA  where ICD10_CODE in (

'D270',
'D271',
'D279'




)
group by YEAR,ETHNICITY
order by YEAR,ETHNICITY;




