-- DATA CLEANING PROCESS

-- 1. Initial Data Review
SELECT * FROM layoffs;

-- PLAN
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Handle Null or Blank Values
-- 4. Remove Unnecessary Columns

-- 2. Create a Staging Table for Data Cleaning
DROP TABLE IF EXISTS layoffs_staging;

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- 3. IDENTIFY DUPLICATES
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
               ORDER BY company
           ) AS row_num
    FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- 4. REMOVE DUPLICATES
DROP TABLE IF EXISTS layoffs_staging2;

CREATE TABLE layoffs_staging2 AS
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
               ORDER BY company
           ) AS row_num
    FROM layoffs_staging
) AS subquery;

DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- Verify that duplicates are removed
SELECT * FROM layoffs_staging2;

-- 5. STANDARDIZING DATA

-- Standardize Company Names (Removing Extra Spaces)
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize Industry Names (Consistent Labels)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize Date Format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 6. HANDLE NULL AND BLANK VALUES

-- Identify Records with Critical Null Values
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Handle Blank Industry Values
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Standardize and Fill Missing Industry Values Based on Matching Company and Location
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- 7. CLEANUP: Remove Temporary Columns
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- FINAL DATA REVIEW
SELECT * FROM layoffs_staging2;
