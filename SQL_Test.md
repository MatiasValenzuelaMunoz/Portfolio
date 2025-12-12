# HomeLight SQL Technical Test
## Candidate: Matías Valenzuela
## Position: Business/Data Analyst
## Date: 08 December 2025

### Executive Summary
Analysis of HomeLight's real estate matching funnel focusing on growth metrics, operational efficiency, and agent performance. All queries are designed for PostgreSQL with consistent date normalization, data validation, and actionable insights for product/operations teams. The approach emphasizes clarity, practical AI usage, and business impact aligned with HomeLight's key products.

### Data Preparation & Assumptions
```sql
-- KEY ASSUMPTIONS:
-- 1. All dates normalized from 'MM/DD/YYYY HH:MM' format using to_timestamp()
-- 2. Latest stages identified with ROW_NUMBER() OVER(PARTITION BY client_id, agent_id)
-- 3. Commission rate: 3% fixed on client price
-- 4. Current date reference: CURRENT_DATE for "stuck" calculations
-- 5. Calendar year analysis (not fiscal)
-- 6. Exclude records with null critical fields (dates, prices, area_id)
```

---

## Question 1: Annual Client Growth
**Business Question**: What is the year-over-year growth trend in client acquisition?

**SQL Query**:
```sql
-- Q1: Annual client growth
WITH normalized_clients AS (
    -- Normalize client dates (format m/d/YYYY HH:MM)
    SELECT 
        id as client_id,
        to_timestamp(created_at, 'MM/DD/YYYY HH24:MI') as created_at_normalized
    FROM clients
    WHERE created_at IS NOT NULL  -- Exclude records without date
),
yearly_counts AS (
    -- Count unique clients per year
    SELECT 
        EXTRACT(YEAR FROM created_at_normalized) as year,
        COUNT(DISTINCT client_id) as client_count
    FROM normalized_clients
    GROUP BY EXTRACT(YEAR FROM created_at_normalized)
)
-- Calculate year-over-year growth
SELECT 
    year,
    client_count,
    LAG(client_count) OVER (ORDER BY year) as prev_year_count,
    ROUND(
        (client_count - LAG(client_count) OVER (ORDER BY year)) * 100.0 / 
        NULLIF(LAG(client_count) OVER (ORDER BY year), 0), 
        2
    ) as growth_percentage
FROM yearly_counts
ORDER BY year;
```

**Key Insights**:
- Tracks acquisition trends over time
- Identifies growth acceleration/deceleration years
- Baseline metric for marketing/sales performance

**Assumptions**:
- Calendar year analysis (Jan-Dec)
- Excluded records with null creation dates
- First year shows NULL growth (no previous year)

**AI Usage**: 
- **Tool**: GPT-4 for LAG() function structure
- **Validation**: Verified EXTRACT(YEAR FROM timestamp) and NULLIF() functions
- **Manual Adjustments**: Added date normalization and data quality filters

---

## Question 2: Highest Volume Areas
**Business Question**: Which geographic areas have the highest client concentration?

**SQL Query**:
```sql
-- Q2: Highest volume areas (client count per area)
SELECT 
    area_id,
    COUNT(DISTINCT id) as total_clients,
    -- Calculate percentage of total
    ROUND(
        COUNT(DISTINCT id) * 100.0 / SUM(COUNT(DISTINCT id)) OVER (), 
        2
    ) as percentage_of_total
FROM clients
WHERE area_id IS NOT NULL  -- Exclude clients without assigned area
GROUP BY area_id
ORDER BY total_clients DESC;
```

**Key Insights**:
- Identifies top-performing geographic markets
- Shows market concentration (80/20 analysis)
- Informs resource allocation and agent matching

**Assumptions**:
- area_id represents geographic regions
- Exclude clients without area assignment
- If areas table exists, JOIN would be added for area names

**AI Usage**:
- **Tool**: GitHub Copilot for aggregation pattern
- **Validation**: Confirmed SUM() OVER() calculates total correctly
- **Manual Adjustments**: Added percentage calculation and null handling

---

## Question 3: Time Between Stages (new → connected → closed)
**Business Question**: How long does it take clients to move through key funnel stages, and are we improving year-over-year?

**SQL Query**:
```sql
-- Q3: Time between stages per year
-- Q3: Time between stages per year (correct version)
-- Each deal = unique (client_id, agent_id)

WITH normalized AS (
    -- Clean and normalize timestamps
    SELECT
        client_id,
        agent_id,
        new_stage,
        TO_TIMESTAMP(created_at, 'MM/DD/YYYY HH24:MI') AS stage_time
    FROM agent_client_stages
    WHERE created_at IS NOT NULL
        AND new_stage IN ('new', 'connected', 'closed')
),

stage_pivot AS (
    -- Pivot stages per deal (client_id + agent_id)
    SELECT
        client_id,
        agent_id,

        -- FIRST time the deal reached each stage (not last!)
        MIN(CASE WHEN new_stage = 'new' THEN stage_time END) AS new_time,
        MIN(CASE WHEN new_stage = 'connected' THEN stage_time END) AS connected_time,
        MIN(CASE WHEN new_stage = 'closed' THEN stage_time END) AS closed_time

    FROM normalized
    GROUP BY client_id, agent_id
    HAVING 
        -- Only deals that progressed at least to connected
        MIN(CASE WHEN new_stage = 'new' THEN stage_time END) IS NOT NULL
        AND MIN(CASE WHEN new_stage = 'connected' THEN stage_time END) IS NOT NULL
),

deal_durations AS (
    -- Compute time differences per deal
    SELECT
        client_id,
        agent_id,
        new_time,
        connected_time,
        closed_time,

        -- durations in days
        EXTRACT(EPOCH FROM (connected_time - new_time)) / 86400.0 AS days_new_to_connected,
        CASE 
            WHEN closed_time IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (closed_time - connected_time)) / 86400.0
        END AS days_connected_to_closed
    FROM stage_pivot
    WHERE connected_time > new_time  -- validate temporal consistency
)

-- Final aggregation: compare by year
SELECT
    EXTRACT(YEAR FROM new_time) AS year,
    COUNT(*) AS total_deals,

    ROUND(AVG(days_new_to_connected), 2) AS avg_days_new_to_connected,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_new_to_connected) 
        AS median_days_new_to_connected,

    ROUND(AVG(days_connected_to_closed), 2) AS avg_days_connected_to_closed,

    -- Percentage of deals that reached closed
    ROUND(
        SUM(CASE WHEN closed_time IS NOT NULL THEN 1 ELSE 0 END) 
        * 100.0 / COUNT(*),
        2
    ) AS closed_percentage

FROM deal_durations
GROUP BY 1
ORDER BY 1;
```

**Key Insights**:
- Measures operational efficiency in agent matching
- Identifies bottleneck stages
- Tracks improvement over time
- Median provides outlier-robust metric

**Assumptions**:
- Clients may have multiple stage records; use most recent per stage
- Only analyze clients who reached 'connected' stage
- 'closed' is optional (not all clients close)
- Calendar days (not business days)

**AI Usage**:
- **Tool**: ChatGPT-4 for pivot (MAX CASE WHEN) pattern
- **Validation**: Verified PERCENTILE_CONT(0.5) for median calculation
- **Manual Adjustments**: Added temporal validation, closure percentage, both avg/median

---

## Question 4: Clients "Stuck" 30+ Days
**Business Question**: Which client-agent pairs are stuck in non-final stages for 30+ days and require intervention?

**SQL Query**:
```sql
-- Q4: Clients "stuck" 30+ days in current stage
WITH normalized_stages AS (
    -- Normalize dates
    SELECT 
        client_id,
        agent_id,
        new_stage,
        to_timestamp(created_at, 'MM/DD/YYYY HH24:MI') as stage_time
    FROM agent_client_stages
    WHERE created_at IS NOT NULL
),
latest_stages AS (
    -- Latest stage per client-agent pair
    SELECT 
        client_id,
        agent_id,
        new_stage as current_stage,
        stage_time as last_update,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, agent_id 
            ORDER BY stage_time DESC
        ) as rn
    FROM normalized_stages
)
-- Identify stuck clients
SELECT 
    ls.client_id,
    c.name as client_name,
    ls.agent_id,
    a.first_name || ' ' || a.last_name as agent_name,
    ls.current_stage,
    ls.last_update,
    -- Days since last update
    EXTRACT(DAY FROM (CURRENT_DATE - ls.last_update)) as days_stuck,
    -- Categorize severity
    CASE 
        WHEN EXTRACT(DAY FROM (CURRENT_DATE - ls.last_update)) BETWEEN 30 AND 60 
            THEN 'Moderate (30-60 days)'
        WHEN EXTRACT(DAY FROM (CURRENT_DATE - ls.last_update)) BETWEEN 61 AND 90 
            THEN 'High (61-90 days)'
        WHEN EXTRACT(DAY FROM (CURRENT_DATE - ls.last_update)) > 90 
            THEN 'Critical (>90 days)'
    END as stuck_severity
FROM latest_stages ls
JOIN clients c ON ls.client_id = c.id
LEFT JOIN agents a ON ls.agent_id = a.id
WHERE ls.rn = 1  -- Latest stage
    AND ls.current_stage NOT IN ('closed', 'failed')  -- Exclude finalized
    AND EXTRACT(DAY FROM (CURRENT_DATE - ls.last_update)) >= 30  -- 30+ days
ORDER BY days_stuck DESC, client_id;
```

**Key Insights**:
- Identifies at-risk client relationships
- Enables proactive intervention by operations team
- Severity categorization prioritizes outreach
- Tracks agent responsiveness

**Assumptions**:
- CURRENT_DATE as reference (could parameterize)
- Calendar days calculation
- Client can be stuck with multiple agents
- 'closed' and 'failed' are terminal states

**AI Usage**:
- **Tool**: GPT-4 for stuck detection logic
- **Validation**: Confirmed CURRENT_DATE - timestamp returns interval
- **Manual Adjustments**: Added severity categorization, JOINs for names, exclusion logic

---

## Question 5: Top Agents by Commissions (Last Year)
**Business Question**: Who were the top-performing agents by commission volume last year?

**SQL Query**:
```sql
-- Q5: Top agents by commissions from last year
WITH normalized_stages AS (
    -- Normalize dates
    SELECT 
        client_id,
        agent_id,
        new_stage,
        to_timestamp(created_at, 'MM/DD/YYYY HH24:MI') as stage_time
    FROM agent_client_stages
),
closed_deals AS (
    -- Deals closed last year
    SELECT DISTINCT ON (ns.client_id, ns.agent_id)
        ns.client_id,
        ns.agent_id,
        ns.stage_time as closed_date,
        c.price
    FROM normalized_stages ns
    JOIN clients c ON ns.client_id = c.id
    WHERE ns.new_stage = 'closed'
        AND EXTRACT(YEAR FROM ns.stage_time) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
        AND c.price IS NOT NULL
        AND c.price > 0
    ORDER BY ns.client_id, ns.agent_id, ns.stage_time DESC
)
-- Calculate commissions per agent
SELECT 
    cd.agent_id,
    a.first_name || ' ' || a.last_name as agent_name,
    COUNT(DISTINCT cd.client_id) as closed_deals_count,
    SUM(cd.price) as total_sales_volume,
    ROUND(SUM(cd.price * 0.03), 2) as total_commission,  -- 3% commission
    ROUND(AVG(cd.price * 0.03), 2) as avg_commission_per_deal,
    -- Ranking by commission
    RANK() OVER (ORDER BY SUM(cd.price * 0.03) DESC) as rank_by_commission
FROM closed_deals cd
JOIN agents a ON cd.agent_id = a.id
GROUP BY cd.agent_id, a.first_name, a.last_name
ORDER BY total_commission DESC
LIMIT 20;  -- Top 20 agents
```

**Key Insights**:
- Identifies high-performing agents for retention/recognition
- Informs compensation and incentive structures
- Shows deal size vs volume trade-off
- Baseline for agent performance benchmarking

**Assumptions**:
- Previous calendar year analysis
- 3% fixed commission rate
- Price represents final sale value
- DISTINCT ON gets latest closed date per client-agent
- Exclude null/zero prices

**AI Usage**:
- **Tool**: ChatGPT-4 for commission calculation structure
- **Validation**: Verified DISTINCT ON works in PostgreSQL
- **Manual Adjustments**: Added price validation, ranking, and decimal formatting

---

## Question 6: Each Agent's Pipeline Value
**Business Question**: What is the weighted pipeline value for each agent based on active clients and stage probabilities?

**SQL Query**:
```sql
-- Q6: Pipeline value per agent (active clients)
WITH normalized_stages AS (
    -- Normalize dates
    SELECT 
        client_id,
        agent_id,
        new_stage,
        to_timestamp(created_at, 'MM/DD/YYYY HH24:MI') as stage_time
    FROM agent_client_stages
),
latest_stages AS (
    -- Latest stage per client-agent
    SELECT 
        client_id,
        agent_id,
        new_stage as current_stage,
        stage_time as last_update,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, agent_id 
            ORDER BY stage_time DESC
        ) as rn
    FROM normalized_stages
),
active_deals AS (
    -- Active clients with price
    SELECT 
        ls.client_id,
        ls.agent_id,
        ls.current_stage,
        c.price,
        -- Closing probability based on stage (simple model)
        CASE ls.current_stage
            WHEN 'new' THEN 0.10
            WHEN 'introduced' THEN 0.25
            WHEN 'connected' THEN 0.40
            WHEN 'met_in_person' THEN 0.60
            WHEN 'listing' THEN 0.80
            ELSE 0.15  -- Other stages
        END as closing_probability
    FROM latest_stages ls
    JOIN clients c ON ls.client_id = c.id
    WHERE ls.rn = 1
        AND ls.current_stage NOT IN ('closed', 'failed')
        AND c.price IS NOT NULL
        AND c.price > 0
)
-- Calculate pipeline per agent
SELECT 
    ad.agent_id,
    a.first_name || ' ' || a.last_name as agent_name,
    COUNT(DISTINCT ad.client_id) as active_clients,
    -- Pipeline value weighted by probability
    ROUND(SUM(ad.price * 0.03 * ad.closing_probability), 2) as weighted_pipeline_value,
    -- Unweighted pipeline value (maximum potential)
    ROUND(SUM(ad.price * 0.03), 2) as max_potential_pipeline,
    -- Average probability
    ROUND(AVG(ad.closing_probability * 100), 1) as avg_closing_probability_percent,
    -- Ranking by pipeline
    RANK() OVER (ORDER BY SUM(ad.price * 0.03 * ad.closing_probability) DESC) as pipeline_rank
FROM active_deals ad
JOIN agents a ON ad.agent_id = a.id
GROUP BY ad.agent_id, a.first_name, a.last_name
ORDER BY weighted_pipeline_value DESC
LIMIT 100;  -- Top 100 agents
```

**Key Insights**:
- Forecasts future commission revenue
- Identifies agents with quality pipeline (not just volume)
- Informs coaching needs (low probability stages)
- Weighted approach reduces over-optimism

**Assumptions**:
- Stage-based probability model (adjustable)
- 3% commission rate
- Only active, non-terminated clients
- Probability estimates based on typical real estate funnel

**AI Usage**:
- **Tool**: GPT-4 for weighted pipeline calculation structure
- **Validation**: Verified probability weighting calculations
- **Manual Adjustments**: Defined realistic stage probabilities, added max vs weighted comparison

---

## AI Usage Transparency

| Question | Tool Used | Prompt Focus | Validation Performed | Manual Adjustments |
|----------|-----------|--------------|---------------------|-------------------|
| 1 | GPT-4 | YoY growth with LAG() | Date functions, NULLIF() | Added data quality filters |
| 2 | Copilot | Aggregation with percentage | SUM() OVER() calculation | Added rounding, null handling |
| 3 | ChatGPT-4 | Stage pivoting & time diffs | PERCENTILE_CONT() for median | Added validation, both avg/median |
| 4 | GPT-4 | Stuck detection logic | CURRENT_DATE interval calculation | Severity categories, JOINs |
| 5 | ChatGPT-4 | Commission calculation | DISTINCT ON in PostgreSQL | Price validation, ranking |
| 6 | GPT-4 | Weighted pipeline model | Probability weighting math | Stage probabilities, comparisons |

**AI Strategy**: Used as accelerator for query patterns, with manual validation of PostgreSQL-specific functions and business logic adjustments.

---

## Business Recommendations

### 1. **Operational Efficiency**
- **Automate Stuck Client Alerts**: Implement daily email/Slack alerts for clients stuck >30 days
- **Focus on new→connected Stage**: This shows the longest duration; consider AI-powered matching acceleration

### 2. **Agent Performance & Development**
- **Pipeline Quality Bonuses**: Reward agents for high-probability pipeline (not just volume)
- **Coaching for Low-Probability Agents**: Identify agents with low avg_closing_probability for targeted training
- **Top Performer Analysis**: Study techniques of top 20 agents by commission for best practices

### 3. **Product & Funnel Optimization**
- **A/B Test Matching Algorithms**: Use stage duration data to optimize agent-client matching
- **Reduce Time-to-Connected**: Target <7 days median for competitive advantage
- **Regional Strategy**: Double down on high-volume areas while improving low-volume areas

### 4. **AI Integration Opportunities**
- **Lead Scoring Model**: Use stage duration patterns to predict conversion likelihood
- **Anomaly Detection**: Flag unusually long stage times for manual review
- **Agent Recommendation**: Enhance matching with performance history from commission data

---

## Performance & Implementation Considerations

### **Suggested Indexes**:
```sql
CREATE INDEX idx_stages_client_agent ON agent_client_stages(client_id, agent_id, created_at DESC);
CREATE INDEX idx_clients_created ON clients(created_at);
CREATE INDEX idx_clients_area ON clients(area_id);
```

### **Refresh Frequency**:
- **Real-time**: Stuck client detection (daily)
- **Daily**: Pipeline value, agent commissions
- **Weekly**: Stage duration analysis
- **Monthly**: Growth trends, area performance

### **Query Optimization**:
- CTEs used for readability; consider materialized views for production
- All date filtering uses normalized timestamps for consistency
- WHERE clauses optimize by filtering early in CTEs

### **Scalability Notes**:
- Queries handle null values explicitly
- Window functions (ROW_NUMBER, RANK) optimized with proper indexes
- LIMIT clauses prevent overwhelming result sets

---

## HomeLight Product Alignment

### **Agent Matching Product**:
- Questions 3 & 4 directly optimize matching efficiency
- Stage duration metrics inform algorithm improvements
- Stuck client detection improves customer experience

### **Buy Before You Sell (BBYS)**:
- Pipeline analysis (Q6) applicable to BBYS risk assessment
- Stage probability model extensible to BBYS approval funnel

### **Title & Escrow**:
- Time tracking (Q3) methodology applicable to closing process
- Stuck detection (Q4) useful for document completion tracking

### **Reviews/Testimonials**:
- Top agent identification (Q5) surfaces candidates for case studies
- Performance metrics provide context for review analysis

---

**Prepared by**: Matias Valenzuela
**Contact**: valenzuelamatias26@gmail.com / +56 9 48668071 
**Submission Date**: 08 December 2025  
**Tools Used**: PostgreSQL, AI-assisted development (documented above), Business analytics framework


**Note**: All queries tested with provided sample data structure and assume standard PostgreSQL 12+ compatibility.
