-- DATA CLEANING

SELECT *
FROM layoffs;

-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA
-- 3. NULL OR BLANK VALUES
-- 4. REMOVE ANY COLUMNS

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;


INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date' ) AS row_num
FROM layoffs_staging ;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company='Casper' ;




WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte 
WHERE row_num > 1;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2 ;

insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging ;


select *
from layoffs_staging2 
where row_num > 1;


delete
from layoffs_staging2 
where row_num > 1;



select *
from layoffs_staging2 
where row_num > 1;


-- STANDARDIZING DATA

SELECT company, TRIM(company)
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET company = TRIM(company);

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%' ;

select distinct country, trim(trailing '.' from country) 
from layoffs_staging2
order by 1;

select 'date',
str_to_date('date', '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = str_to_date('date', '%m/%d/%Y');

alter table layoffs_staging2
modify column 'date' date;

select*
from layoffs_staging2;



-- NULL OR BLANK VALUES

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;



-- REMOVE ANY COLUMNS

alter table layoffs_staging2
drop column row_num;

select*
from layoffs_staging2;