You are generating a statistical release about the Consumer Price Index
for Nova Scotia, in the style of a government economics and statistics
division. The release should be factual, precise, and analytically useful
for executives, analysts, and policymakers.

## DATA

{data_json}

## FORMAT

Title: "CONSUMER PRICE INDEX, {ref_month_upper} {ref_year}"

**Section 1: Year-over-year change**

Compare the current reference month to the same month one year ago.
Report for:
- Nova Scotia All-items: YoY rate, direction of change from the prior month's YoY rate
- Canada All-items: YoY rate (as national comparison)
- Halifax All-items: YoY rate (if available)
- Nova Scotia All-items excluding food and energy: YoY rate
- Canada All-items excluding food and energy: YoY rate

Then break down the major CPI components for Nova Scotia:
- Food
- Shelter
- Transportation
- Energy
- Clothing and footwear
- Health and personal care

For each component, report the YoY percentage change.

**Section 2: Month-over-month change**

Compare the current reference month to the immediately preceding month.
Report for:
- Nova Scotia All-items: MoM change
- Canada All-items: MoM change
- Halifax All-items: MoM change (if available)

Note: CPI data is not seasonally adjusted, so month-over-month comparisons
should be presented with appropriate context about seasonal patterns.

**Footer:**

Source: Statistics Canada, Table 18-10-0004-01, Consumer Price Index, monthly, not seasonally adjusted.

AI-Generated Draft — Not Reviewed by an Economist

## RULES

- Report numbers exactly as provided in the data. Do not round differently than the source.
- Use "increased" for positive change, "decreased" for negative change, "was unchanged" for zero.
- When comparing rates (e.g., "inflation was 3.2% in October, up from 2.8% in September"), state percentage POINT changes, not percentage changes of rates.
- Calculate year-over-year percentage change as: ((current - year_ago) / year_ago) * 100, rounded to one decimal place.
- Calculate month-over-month percentage change as: ((current - prior_month) / prior_month) * 100, rounded to one decimal place.
- Do not speculate on causes unless the data directly supports it.
- Do not use adjectives like "strong," "weak," "surprising," or "disappointing."
- Factual, neutral, professional tone throughout.
- If a data point is missing or unavailable, note it and move on. Do not fabricate values.
- End with: "AI-Generated Draft — Not Reviewed by an Economist"
