-- Data Cleaning


SELECT *
FROM `marketing data`;

-- Created a new table to prevent permanent mistakes in the raw table

CREATE TABLE marketing_data_staging
LIKE `marketing data`;

SELECT *
FROM marketing_data_staging;

INSERT marketing_data_staging
SELECT *
FROM `marketing data`;

-- Removing duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Campaign, `Date`, `City/Location`, Latitude, Longitude, `Channel`, Device, Ad, Impressions, CTR, Clicks, `Daily Average CPC`, `Spend, GBP`, Conversions, `Total conversion value, GBP`, `Likes (Reactions)`, Shares, Comments) AS row_num
FROM marketing_data_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Campaign, `Date`, `City/Location`, Latitude, Longitude, `Channel`, Device, Ad, Impressions, CTR, Clicks, `Daily Average CPC`, `Spend, GBP`, Conversions, `Total conversion value, GBP`, `Likes (Reactions)`, Shares, Comments) AS row_num
FROM marketing_data_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Standardising data

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM marketing_data_staging;


UPDATE marketing_data_staging
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE marketing_data_staging
MODIFY COLUMN `date` DATE;


SELECT *
FROM marketing_data_staging;

--- Campaign Performance

WITH RankedCampaign AS(
    SELECT Campaign,
           SUM(Impressions) AS total_impressions,
           SUM(Clicks) AS total_clicks,
           SUM(Conversions) AS total_conversions,
           RANK() OVER(ORDER BY SUM(Impressions) DESC) AS impressions_rank,
           RANK() OVER(ORDER BY SUM(Clicks) DESC) AS clicks_rank,
           RANK() OVER(ORDER BY SUM(Conversions) DESC) AS conversions_rank
	FROM marketing_data_staging
    GROUP BY Campaign
)
SELECT 'Highest Impressions' AS Metric, Campaign, total_impressions AS Value FROM RankedCampaign WHERE impressions_rank = 1
UNION ALL
SELECT 'Highest Clicks' AS Metric, Campaign, total_clicks AS Value FROM RankedCampaign WHERE clicks_rank = 1 
UNION ALL
SELECT 'Highest Conversions' AS Metric, Campaign, total_conversions AS Value FROM RankedCampaign WHERE conversions_rank = 1;


SELECT Campaign, 
    ROUND(AVG(NULLIF(`Daily Average CPC`, 0)), 2) AS avg_cpc,  -- Handle zero values
    ROUND(AVG(REPLACE(`CTR`, '%', '') * 1.0), 2) AS avg_ctr  -- Convert CTR from percentage
FROM marketing_data_staging
GROUP BY Campaign;


--- Channel Effectiveness

SELECT `Channel`, 
    ROUND(
        CASE 
            WHEN COALESCE(SUM(`Spend, GBP`), 0) = 0 THEN 0  
            ELSE ((COALESCE(SUM(`Total conversion value, GBP`), 0) - COALESCE(SUM(`Spend, GBP`), 0)) 
                / NULLIF(COALESCE(SUM(`Spend, GBP`), 0), 0)) * 100
        END, 2
    ) AS percentage_ROI
FROM marketing_data_staging
GROUP BY `Channel`
ORDER BY percentage_ROI DESC;


SELECT `Channel`,
	 ROUND(SUM(Impressions), 2) AS total_impressions,
     ROUND(SUM(Clicks), 2) AS total_clicks,
     ROUND(SUM(Conversions), 2) AS total_conversions
FROM marketing_data_staging
GROUP BY `Channel`
ORDER BY total_impressions;

--- Geographical insights

SELECT `City/Location`,
      ROUND(SUM(`Likes (Reactions)`), 2) AS total_likes,
      ROUND(SUM(Shares), 2) AS total_shares,
      ROUND(SUM(Comments), 2) AS total_comments
FROM marketing_data_staging
GROUP BY `City/Location`
ORDER BY total_likes DESC;

SELECT `City/Location`,
     SUM(Clicks) AS total_clicks,
     SUM(Conversions) AS total_conversions,
     ROUND(SUM(Conversions) / nullif(SUM(Clicks), 0) * 100, 2) AS Conversion_rate
FROM marketing_data_staging
GROUP BY `City/Location`
ORDER BY Conversion_rate DESC;

--- Decive Performance

SELECT Device, 
    SUM(Impressions) AS total_impressions, 
    SUM(Clicks) AS total_clicks, 
    ROUND((SUM(Clicks) / NULLIF(SUM(Impressions), 0)) * 100, 2) AS ctr_percentage,
    SUM(Conversions) AS total_conversions, 
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) AS conversion_rate,
    ROUND(SUM(`Spend, GBP`) / NULLIF(SUM(Clicks), 0), 2) AS avg_cpc
FROM marketing_data_staging
GROUP BY Device 
ORDER BY total_impressions DESC;

SELECT Device, 
    SUM(Clicks) AS total_clicks, 
    SUM(Conversions) AS total_conversions, 
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) AS conversion_rate
FROM marketing_data_staging
GROUP BY Device
ORDER BY conversion_rate DESC
LIMIT 1;


--- Ad level analysis

SELECT Ad, 
    SUM(Impressions) AS total_impressions, 
    SUM(Clicks) AS total_clicks, 
    ROUND(AVG(REPLACE(CTR, '%', '') * 1.0), 2) AS avg_ctr_percentage,  
    SUM(`Likes (Reactions)`) AS total_likes, 
    SUM(Shares) AS total_shares, 
    SUM(Comments) AS total_comments, 
    (SUM(`Likes (Reactions)`) + SUM(Shares) + SUM(Comments)) AS total_engagement,
    SUM(Conversions) AS total_conversions, 
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) AS conversion_rate
FROM marketing_data_staging
GROUP BY Ad
ORDER BY total_engagement DESC, conversion_rate DESC;

SELECT Ad, Device,
    SUM(Impressions) AS total_impressions, 
    SUM(Clicks) AS total_clicks, 
    ROUND(AVG(REPLACE(`CTR`, '%', '') * 1.0), 2) AS avg_ctr_percentage,  
    SUM(`Likes (Reactions)`) AS total_likes, 
    SUM(Shares) AS total_shares, 
    SUM(Comments) AS total_comments, 
    (SUM(`Likes (Reactions)`) + SUM(Shares) + SUM(Comments)) AS total_engagement,
    SUM(Conversions) AS total_conversions, 
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) AS conversion_rate,
    ROUND(SUM(`Spend, GBP`) / NULLIF(SUM(Clicks), 0), 2) AS avg_cpc
FROM marketing_data_staging
GROUP BY Ad, Device
ORDER BY total_engagement DESC, conversion_rate DESC;

--- ROI Calculation
SELECT Campaign, `Channel`, Device,
	SUM(`Spend, GBP`) AS total_spend,
    SUM(`Total conversion value, GBP`) AS total_revenue,
    ROUND(
       CASE
		  WHEN COALESCE(SUM(`Spend, GBP`), 0) = 0 THEN 0
          ELSE ((COALESCE(SUM(`Total conversion value, GBP`), 0) - COALESCE(SUM(`Spend, GBP`), 0))
               / NULLIF(COALESCE(SUM(`Spend, GBP`), 0), 0)) * 100
		END, 2
	) AS ROI_percentage
FROM marketing_data_staging
GROUP BY  Campaign, `Channel`, Device
ORDER BY ROI_percentage DESC;


SELECT Campaign, 
    SUM(`Spend, GBP`) AS total_spend, 
    SUM(`Total conversion value, GBP`) AS total_conversion_value
FROM marketing_data_staging
GROUP BY Campaign
ORDER BY total_spend DESC;


--- Time series analysis

SELECT 
    DATE_FORMAT(`date`, '%Y-%m') AS month,  -- Grouping by Year-Month
    SUM(Impressions) AS total_impressions, 
    SUM(Clicks) AS total_clicks, 
    ROUND(AVG(REPLACE(`CTR`, '%', '') * 1.0), 2) AS avg_ctr_percentage,  
    SUM(Conversions) AS total_conversions, 
    ROUND((SUM(Conversions) / NULLIF(SUM(Clicks), 0)) * 100, 2) AS conversion_rate,
    SUM(`Spend, GBP`) AS total_spend,
    SUM(`Total conversion value, GBP`) AS total_revenue
FROM marketing_data_staging
GROUP BY month
ORDER BY month;

-- Select all distinct campaigns
SELECT DISTINCT(Campaign)
FROM marketing_data_staging;


