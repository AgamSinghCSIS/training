-- SQL Advanced assignment
-- Created by Agampaul Singh 
-- All questions are attempted according to my interpretation of them, 
-- if there is any questions wrong please provide feedback. 

-- Q1
SELECT SUM(population) AS total_population 
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085';

-- Q2
SELECT gender, SUM(population) AS total_population 
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
GROUP BY gender;

-- Q3: Not showing null age groups 
-- Only showing valid age group info 
(
  SELECT gender,
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group, 
  MAX(population) AS max_headcount
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
  AND gender = 'male'
  AND minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL
GROUP BY gender,age_group
ORDER BY max_headcount DESC
LIMIT 1
)
UNION ALL
( SELECT gender,
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group, 
  MAX(population) AS max_headcount
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
  AND gender = 'female'
  AND minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL
GROUP BY gender,age_group
ORDER BY max_headcount DESC
LIMIT 1); 

-- Q.4 Not showing null age groups 
-- Only showing valid age group info 
SELECT 
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group,
  MAX(population) AS max_male_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
  AND gender = 'male'
  AND minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL 
GROUP BY age_group, gender
ORDER BY max_male_population DESC
LIMIT 1;

-- Q.5 Same as Q4, invalid age_group data in filtered out 

SELECT 
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group,
  MAX(population) AS max_female_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
  AND gender = 'female'
  AND minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL 
GROUP BY age_group, gender
ORDER BY max_female_population DESC
LIMIT 1;

-- Q.6 Again filtering out the null data in gender to get 
-- max population by zipcode for male and female 

(SELECT zipcode, gender, MAX(population) AS max_population 
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE gender = 'male'
GROUP BY zipcode, gender
ORDER BY max_population DESC
LIMIT 1)
UNION ALL
(SELECT zipcode, gender, MAX(population) AS max_population 
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE gender = 'female'
GROUP BY zipcode, gender
ORDER BY max_population DESC
LIMIT 1
);

-- Q.7 
( SELECT DISTINCT
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group,
  gender,
  SUM(population) AS highest_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL
  AND gender = 'male'
GROUP BY age_group, gender 
ORDER BY highest_population DESC
LIMIT 5)

UNION ALL 

( SELECT DISTINCT
  CONCAT(minimum_age, ' - ', maximum_age) AS age_group,
  gender,
  SUM(population) AS highest_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE minimum_age IS NOT NULL
  AND maximum_age IS NOT NULL
  AND gender = 'female'
GROUP BY age_group, gender 
ORDER BY highest_population DESC
LIMIT 5);

-- Q.8 
SELECT zipcode, population AS top_5_female_populations
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE gender = 'female'
ORDER by population DESC
LIMIT 5;


-- Q.9 
SELECT zipcode, population AS lowest_10_male_populations
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE gender = 'male'
ORDER by population ASC
LIMIT 10;

