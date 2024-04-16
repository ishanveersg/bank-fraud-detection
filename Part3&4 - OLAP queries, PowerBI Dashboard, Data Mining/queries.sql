-- Rollup: payment_type_totalFraud

CREATE VIEW payment_type_totalFraud AS (
SELECT
    t.payment_type,
    COUNT(*) AS num_transactions,
    SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions,
    AVG(f.name_email_similarity) AS avg_name_email_similarity,
    AVG(f.velocity_6h) AS avg_velocity_6h,
    AVG(f.velocity_24h) AS avg_velocity_24h,
    AVG(f.velocity_4w) AS avg_velocity_4w
FROM
    transaction_dimension AS t
JOIN
    fact_table AS f ON t.transaction_id = f.transaction_id
GROUP BY
    t.payment_type);

-- Roll-up: Employment_status_fraud

CREATE VIEW Employment_status_fraud AS (
SELECT
    a.Employment_status,
    COUNT(*) AS num_transactions,
    SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
FROM
    applicant_dimension AS a
JOIN
    fact_table AS f ON a.applicant_id = f.applicant_id
GROUP BY
    a.Employment_status);

--------------------------------------------------------------------
-- Drill-down: age_group_fraud

CREATE VIEW age_group_fraud AS (
    SELECT
        (CASE 
            WHEN a.customer_age = '10' THEN '10'
            WHEN a.customer_age = '20' THEN '20'
            WHEN a.customer_age = '30' THEN '30'
            WHEN a.customer_age = '40' THEN '40'
            WHEN a.customer_age = '50' THEN '50'
            ELSE '60' END ) AS age_group,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    GROUP BY
        age_group
);

-- Drill-down: fraud_transactions_by_income

CREATE VIEW fraud_transactions_by_income AS (
    SELECT
        (CASE 
            WHEN a.Income >= 0.1 AND a.Income < 0.3 THEN 'Low'
            WHEN a.Income >= 0.3 AND a.Income < 0.6 THEN 'Medium'
            ELSE 'High' END ) AS income_group,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    GROUP BY
        income_group
);

SELECT * from age_group_fraud WHERE age_group = '20';

--------------------------------------------------------------------
-- Slice: payment_type_totalFraud
CREATE VIEW payment_type_slice AS (
    SELECT
        a.customer_age,
        d.device_os,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        fact_table AS f
    JOIN
        applicant_dimension AS a ON f.applicant_id = a.applicant_id
    JOIN
        device_dimension AS d ON f.device_id = d.device_id
    JOIN
        transaction_dimension AS t ON f.transaction_id = t.transaction_id
    WHERE
        t.payment_type = 'AB'
    GROUP BY
        a.customer_age, d.device_os
);

-- Slice: fraud_by_has_other_cards

CREATE VIEW fraud_by_has_other_cards AS (
    SELECT
        a.Has_other_cards,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        fact_table AS f
    JOIN
        applicant_dimension AS a ON f.applicant_id = a.applicant_id
    WHERE
        a.Housing_status = 'BA'
    GROUP BY
        a.Has_other_cards
);

--------------------------------------------------------------------

-- Dice: payment_type_totalFraud
CREATE VIEW payment_type_dice AS (
    SELECT
        a.customer_age,
        t.source,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        fact_table AS f
    JOIN
        applicant_dimension AS a ON f.applicant_id = a.applicant_id
    JOIN
        device_dimension AS d ON f.device_id = d.device_id
    JOIN
        transaction_dimension AS t ON f.transaction_id = t.transaction_id
    WHERE
        t.payment_type = 'AB' AND d.device_os = 'windows'
    GROUP BY
        a.customer_age, t.source
);


-- Dice: fraud_by_age_and_proposed_credit

CREATE VIEW fraud_by_age_and_proposed_credit AS (
    SELECT
        a.customer_age,
        (CASE 
            WHEN a.proposed_credit_limit >= 200 AND a.proposed_credit_limit < 1000 THEN 'Low'
            WHEN a.proposed_credit_limit >= 1000 AND a.proposed_credit_limit < 1500 THEN 'Medium'
            ELSE 'High' END ) AS proposed_credit_group,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        fact_table AS f
    JOIN
        applicant_dimension AS a ON f.applicant_id = a.applicant_id
    JOIN
        transaction_dimension AS t ON f.transaction_id = t.transaction_id
    WHERE
        t.source = 'INTERNET' AND a.Employment_status = 'CA'
    GROUP BY
        a.customer_age, proposed_credit_group
);

-------------------------------------------------------------------
-- Combining OLAP  queries
-- Query 1: Roll up and dice 
CREATE VIEW fraud_by_income_and_age AS (
    SELECT
        a.income_group,
        a.age_group,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions,
        AVG(f.name_email_similarity) AS avg_name_email_similarity,
        AVG(f.velocity_6h) AS avg_velocity_6h,
        AVG(f.velocity_24h) AS avg_velocity_24h,
        AVG(f.velocity_4w) AS avg_velocity_4w
    FROM
        (SELECT
            applicant_id,
            CASE
                WHEN income >= 0.1 AND income < 0.4 THEN 'Low'
                WHEN income >= 0.4 AND income < 0.7 THEN 'Medium'
                ELSE 'High'
            END AS income_group,
            CASE
                WHEN customer_age >= 10 AND customer_age < 30 THEN 'Young'
                WHEN customer_age >= 30 AND customer_age < 50 THEN 'Middle-aged'
                ELSE 'Old'
            END AS age_group
        FROM
            applicant_dimension) AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    GROUP BY
        a.income_group, a.age_group
    ORDER BY
        a.income_group, a.age_group
);

-- Query 2 : Roll up and dice 

CREATE VIEW age_group_and_employment_status AS (
    SELECT
        a.age_group,
        a.employment_status,
        d.device_os,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions,
        AVG(f.name_email_similarity) AS avg_name_email_similarity,
        AVG(f.velocity_6h) AS avg_velocity_6h,
        AVG(f.velocity_24h) AS avg_velocity_24h,
        AVG(f.velocity_4w) AS avg_velocity_4w
    FROM
        (SELECT
            applicant_id,
            employment_status,
            CASE
                WHEN customer_age >= 10 AND customer_age < 30 THEN 'Young'
                WHEN customer_age >= 30 AND customer_age < 50 THEN 'Middle-aged'
                ELSE 'Old'
            END AS age_group
        FROM
            applicant_dimension) AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    JOIN
        device_dimension AS d ON f.device_id = d.device_id
    WHERE
        d.device_os = 'windows'
    GROUP BY
        a.age_group, a.employment_status,
        d.device_os
    ORDER BY
        a.age_group, a.employment_status
);

-- Query 3: Rollup and Slice

CREATE VIEW employment_status_device_os_transactions_ac_payment AS  (
    WITH filtered_data AS (
    SELECT a.employment_status, d.device_os, t.payment_type, ft.fraud_bool
    FROM fact_table ft
    JOIN applicant_dimension a ON ft.applicant_id = a.applicant_id
    JOIN transaction_dimension t ON ft.transaction_id = t.transaction_id
    JOIN device_dimension d ON ft.device_id = d.device_id
    WHERE t.payment_type = 'AC'
    )
    SELECT employment_status, device_os, COUNT(*) AS total_transactions, SUM(CASE WHEN fraud_bool THEN 1 ELSE 0 END) AS total_fraud_transactions
    FROM filtered_data
    GROUP BY ROLLUP (employment_status, device_os)
);

-- Query 4: Drill-Down and Dice


CREATE VIEW employment_housing_device_avg_days_internet_ab_payment AS (
    WITH filtered_data AS (
    SELECT a.employment_status, a.housing_status, d.device_os, t.source, t.payment_type, t.days_since_request
    FROM fact_table ft
    JOIN applicant_dimension a ON ft.applicant_id = a.applicant_id
    JOIN transaction_dimension t ON ft.transaction_id = t.transaction_id
    JOIN device_dimension d ON ft.device_id = d.device_id
    WHERE t.source = 'INTERNET' AND t.payment_type = 'AB'
    )
    SELECT employment_status, housing_status, device_os, AVG(days_since_request) AS avg_days_since_request
    FROM filtered_data
    GROUP BY employment_status, housing_status, device_os
);

-------------------------------------------------------------------------------------
-------Iceberg query 
WITH age_groups AS (
    SELECT
        customer_age,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    GROUP BY
        a.customer_age
)
SELECT
    customer_age,
    num_fraud_transactions
FROM
    age_groups
ORDER BY
    num_fraud_transactions DESC
LIMIT 5;



------------------------------------------------------------------------------

--------- Windowing Query
WITH age_groups AS (
    SELECT
        customer_age,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    WHERE
        f.velocity_6h >= 0
    GROUP BY
        a.customer_age
)
SELECT
    customer_age,
    num_fraud_transactions,
    AVG(num_fraud_transactions) OVER() AS avg_fraud_transactions,
    RANK() OVER (ORDER BY num_fraud_transactions DESC) AS fraud_rank
FROM
    age_groups;

-------- Window Query (ALTERNATIVE)

WITH age_groups AS (
    SELECT
        customer_age,
        COUNT(*) AS num_transactions,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    WHERE
        f.velocity_6h >= 0
    GROUP BY
        a.customer_age
)
SELECT
    customer_age,
    num_fraud_transactions,
    num_transactions,
    (num_fraud_transactions::float / num_transactions::float) AS avg_fraud_transactions_by_age,
    RANK() OVER (ORDER BY num_fraud_transactions DESC) AS fraud_rank
FROM
    age_groups;

---------------------------------------------------------------------------
------- Using the Window clause 

WITH age_groups AS (
    SELECT
        customer_age,
        SUM(CASE WHEN f.fraud_bool = 't' THEN 1 ELSE 0 END) AS num_fraud_transactions
    FROM
        applicant_dimension AS a
    JOIN
        fact_table AS f ON a.applicant_id = f.applicant_id
    GROUP BY
        a.customer_age
)
SELECT
    a1.customer_age,
    a1.num_fraud_transactions,
    a2.num_fraud_transactions AS previous_age_fraud_transactions,
    a3.num_fraud_transactions AS next_age_fraud_transactions
FROM
    age_groups AS a1
LEFT JOIN
    age_groups AS a2 ON a1.customer_age = a2.customer_age + 10
LEFT JOIN
    age_groups AS a3 ON a1.customer_age = a3.customer_age - 10;



