-- Data Quality Checks
-- Domain: IoT / Wearable Activity & Sleep Data
-- Dataset: combined activity data

-- DQ-01
-- Completeness
-- Id is not null

SELECT
    'DQ-01' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE id IS NULL;


-- DQ-02
-- Validity
-- ActivityDay is a valid date

SELECT
    'DQ-02' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE activity_day IS NULL;


-- DQ-03
-- Uniqueness
-- Id + ActivityDay must be unique

SELECT
    'DQ-03' AS rule_id,
    COUNT(*) AS violations
FROM (
    SELECT
        id,
        activity_day
    FROM activity_data
    GROUP BY id, activity_day
    HAVING COUNT(*) > 1
) duplicates;


-- DQ-04
-- Validity
-- Numeric values must be non-negative

SELECT
    'DQ-04' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE
    calories < 0
    OR minutes_instances < 0
    OR sedentary_minutes < 0
    OR light_active_minutes < 0
    OR moderately_active_minutes < 0
    OR very_active_minutes < 0
    OR steps < 0
    OR total_minutes_asleep < 0
    OR half_dream < 0
    OR minutes_in_bed < 0
    OR sleeps_instances < 0;


-- DQ-05
-- Consistency
-- Activity minutes must equal minutes_instances

SELECT
    'DQ-05' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE
    sedentary_minutes
    + light_active_minutes
    + moderately_active_minutes
    + very_active_minutes
    <> minutes_instances;


-- DQ-06
-- Validity
-- minutes_instances must not exceed 1440 minutes per day

SELECT
    'DQ-06' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE minutes_instances > 1440;


-- DQ-07
-- Completeness
-- Full daily observation coverage

SELECT
    'DQ-07' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE minutes_instances < 1440;


-- DQ-08
-- Consistency
-- SleepsInstances > 0 implies TotalMinutesAsleep > 0

SELECT
    'DQ-08' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE
    sleeps_instances > 0
    AND total_minutes_asleep = 0;


-- DQ-09
-- Completeness
-- TotalMinutesAsleep > 0 implies HalfDream > 0

SELECT
    'DQ-09' AS rule_id,
    COUNT(*) AS violations
FROM activity_data
WHERE
    total_minutes_asleep > 0
    AND half_dream = 0;