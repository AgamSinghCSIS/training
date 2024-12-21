-- Advanced SQL assignment; 2nd Attempt 
-- Changes: Using Window functions instead of Set operations and limit 
-- Made by Agampaul Singh; Submitted to Bhargav 

-- Q1
SELECT SUM(population) AS total_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085';

-- Q2 
SELECT gender, SUM(population) AS total_population
FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
WHERE zipcode = '94085'
GROUP BY gender;

-- Q3 
SELECT DISTINCT age_group FROM (
  SELECT age_group, gender,
    RANK() OVER(ORDER BY max_zip DESC ) AS rank
  FROM (
    SELECT DISTINCT gender,
        CONCAT(minimum_age, ' - ', maximum_age) AS age_group, 
        SUM(population) OVER(PARTITION BY CONCAT(minimum_age, ' - ', maximum_age)) AS max_zip,
    FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
    WHERE zipcode = '94085'
        AND CONCAT(minimum_age, ' - ', maximum_age) IS NOT NULL
        AND gender IS NOT NULL 
  )) 
WHERE rank = 1;

-- Q4 
SELECT age_group, population AS max_male_headcount FROM (
  SELECT DISTINCT CONCAT(minimum_age, ' - ', maximum_age) AS age_group, 
      population,
      RANK() OVER(ORDER BY population DESC) AS male_rank
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE zipcode = '94085'
      AND CONCAT(minimum_age, ' - ', maximum_age) IS NOT NULL
      AND gender = 'male'
)
WHERE male_rank = 1; 

-- Q.5 
SELECT age_group, population AS max_female_headcount FROM (
  SELECT DISTINCT CONCAT(minimum_age, ' - ', maximum_age) AS age_group, 
      population,
      RANK() OVER(ORDER BY population DESC) AS female_rank
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE zipcode = '94085'
      AND CONCAT(minimum_age, ' - ', maximum_age) IS NOT NULL
      AND gender = 'female'
)
WHERE female_rank = 1;

--Q6 Assuming you need zipcode having highest combined population in US 
-- Upon Examining the data, rows with gender value NULL have cumulative 
-- population of male + females for all age groups combined 
SELECT zipcode, population FROM (
  SELECT zipcode, population,
    RANK() OVER(ORDER BY population DESC) AS rank_pop
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE gender IS NULL)
WHERE rank_pop = 1;

-- Now same Q6 but seperately for both male and females 
-- Zipcode with highest male and female population 
SELECT zipcode, gender, population FROM (
  SELECT zipcode, population, gender,
    RANK() OVER(PARTITION BY gender ORDER BY population DESC) AS rank_pop
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE gender IS NOT NULL
  AND minimum_age IS NULL
  AND maximum_age IS NULL)
WHERE rank_pop = 1;

-- Q.7 5 zipcodes with Combined male and female population 
-- highest 
SELECT * FROM (
SELECT age_group, combined_pop, 
     RANK() OVER(ORDER BY combined_pop DESC) AS rank_by_age 
FROM (
  SELECT DISTINCT CONCAT(minimum_age, ' - ', maximum_age) AS age_group,
    SUM(population) OVER(PARTITION BY CONCAT(minimum_age, ' - ', maximum_age)) AS combined_pop
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE gender IS NOT NULL
    AND minimum_age IS NOT NULL
  )
)
WHERE rank_by_age <= 5
ORDER BY rank_by_age;

-- Q.8  
SELECT * FROM (
  SELECT zipcode, 
    population, 
    RANK() OVER(ORDER BY population DESC) AS rank_female_pop
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE gender = 'female'   -- records with these combinations 
    AND minimum_age IS NUll -- describe total female population
    AND maximum_age IS NULL -- in that zipcode irrespective of age 
) 
WHERE rank_female_pop <= 5
ORDER BY rank_female_pop ASC;

-- Q.9 
SELECT * FROM (
  SELECT zipcode, 
    population, 
    RANK() OVER(ORDER BY population ASC) AS low_rank_male_pop
  FROM bigquery-public-data.census_bureau_usa.population_by_zip_2010
  WHERE gender = 'male'   -- records with these combinations 
    AND minimum_age IS NUll -- describe total MALE population
    AND maximum_age IS NULL -- in that zipcode irrespective of age 
) 
WHERE low_rank_male_pop <= 10
ORDER BY low_rank_male_pop ASC;
-- Prints 152 zipcodes with 0 male population All ranked 1
-- Could use ROW_NUMBER() instead of RANK() to show only 10 zipcodes. 