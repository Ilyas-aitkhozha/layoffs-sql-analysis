-- DATA cleaning
-- there are 4 step that i ll be doing in this cleaning
-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Handle Null or Blank values
-- 4. Remove any columns or rows

SELECT * 
from layoffs_staging;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
select * 
from layoffs;


# first step in order to clean data is to remove all duplicates

WITH duplicates_cte as
(#with partition by, we can identify if we have identical rows, and if its > 1, we should delete them
SELECT *,
ROW_NUMBER() OVER(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_n
from layoffs_staging
)
select * from duplicates_cte
where row_n > 1;




#creating new table, in order to delete duplicates (pretty straightforward method, but we cant delete within CTE)
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;
#inserting all of our data in the new table
insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

# checking if duplicates exists(always the best practice)
select * 
from layoffs_staging2
where row_num > 1;
#deleting duplicates
DELETE
from layoffs_staging2
where row_num > 1;


# Second step Is to standartize data (finding issues in your data and then fixing it)

SELECT company, TRIM(company)
from layoffs_staging2;
#using trim to delete whitespaces from both sides	
UPDATE layoffs_staging2
set company = TRIM(company);


SELECT distinct industry
from layoffs_staging2
order by 1;
#In the raw dataset, we have Crypto, Cryptocurrency, Crypto Currency, and actually we need to name them one thing, because its basically the same thing 
SELECT * from layoffs_staging2
where industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry like 'Crypto%';
#now we have only one Crypto industry


#now we are checking for errors in Country
SELECT DISTINCT Country
from layoffs_staging2
order by 1;
#we saw that here we have United States and United States. So difference only with one dot, and we need to set it to only United States

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' from country)
where country like 'United States%';
#now we deleted it by using TRIM, and trailing inside of it

#now we want to convert date from string to data
select  `date`
from layoffs_staging2;
#we updated it by using str_to_date, to format 4 digits year, month and day
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;



# now is the third step, Removing all null or blank data
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

#checking, if we have null and blank in industry
select * from layoffs_staging2
where industry is null or industry = '';

#now we want to update industry to null where its blank (in order to make operations easier)
UPDATE layoffs_staging2
SET industry = NULL
where industry = '';
#checking where we have null in first, and not null in the second one
select t1.industry, t2.industry
from layoffs_staging t1
join layoffs_staging t2
	on t1.company = t2.company
where t1.industry is null and t2.industry is not null;

#update if we have null in first, and not null in the second one
UPDATE layoffs_staging2 t1
join layoffs_staging t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;

#now we see that we have data, where there is no data in total_laid_off and percentage_laid_off
#we can delete this data, just because they have no valuable information. for example if we had employees_before_laid_off, 
#then we could calculate this, but because we dont have such information, they are useless to us
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;
# now when we cleaned this data, we can do the main thing.



-- Exploratory Data Analysis

select * from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off) # if percentage_laid_off equals to one, that means 100 % percent of workers were laid_off
from layoffs_staging2;

#checking how much money was put into company, but its just stopped working
SELECT *
FROM layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions DESC;

#we are checking from what period this dataset is giving us data (2020-03-11; 2023-03-06)
select MIN(`date`), MAX(`date`)
from layoffs_staging2;

#from below we will see, how much of laid off there was, in different category (company, industry, country, year and stage)
select company, SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by company
order by 2 desc;

select industry, SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by industry
order by 2 desc;

select country, SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by country
order by 2 desc;

select YEAR(`date`), SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;

select stage, SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by stage
order by 2 desc;

#now we want to see, how much there were laid offs per month, and for comparison we will do it right next to the rolling total
with Rolling_total as
(
select substring(`date`, 1, 7) as `month`, SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 
)
select `month`, sum_of_laid_off, sum(sum_of_laid_off) over(order by `month`) as rolling_total
from Rolling_total;
 

# below we will see, top 5 total laid off, for each category (company,industry, country, stage) 
with company_year (company, years, total_laid_off )as 
(
select company, YEAR(`date`), SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by company, YEAR(`date`)
),
company_year_rank as 
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5;

with industry_year (industry, years, total_laid_off)as 
(
select industry, YEAR(`date`), SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by industry, YEAR(`date`)
),
industry_year_rank as 
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) ranking
from industry_year
where years is not null
)
select *
from industry_year_rank
where ranking <= 5;

with country_year (country, years, total_laid_off)as 
(
select country, YEAR(`date`), SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by country, YEAR(`date`)
),
country_year_rank as 
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) ranking
from country_year
where years is not null
)
select *
from country_year_rank
where ranking <= 5;

with stage_year (stage, years, total_laid_off)as 
(
select stage, YEAR(`date`), SUM(total_laid_off) as sum_of_laid_off
from layoffs_staging2
group by stage, YEAR(`date`)
),
stage_year_rank as 
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) ranking
from stage_year
where years is not null
)
select *
from stage_year_rank
where ranking <= 5;


