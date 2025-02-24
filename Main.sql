WITH act1 AS (
SELECT
        client_uuid AS user_uuid,
        CASE WHEN client_nationality = 'China' THEN 'CN'
        WHEN client_nationality = 'Taiwan China' THEN 'TW'
        WHEN client_nationality = 'Hong Kong,China' THEN 'HK'
        WHEN client_nationality = 'Macao, China' THEN 'MO'
        END AS user_nationality,
        client_type AS user_type,
        activity_type,
        SUM(crm_deposit) AS crm_deposit,
        SUM(crm_withdrawal) AS crm_withdrawal,
        SUM(crm_net_deposit) AS crm_net_deposit,
        SUM(mt_net_deposit) AS mt_net_deposit,
        SUM(qualified_trading_volume) AS trading_lots
FROM
        <db table>
WHERE
        activity_type = 'activity 1'
GROUP BY
        client_uuid,
        CASE WHEN client_nationality = 'China' THEN 'CN'
        WHEN client_nationality = 'Taiwan China' THEN 'TW'
        WHEN client_nationality = 'Hong Kong,China' THEN 'HK'
        WHEN client_nationality = 'Macao, China' THEN 'MO'
        END,
        client_type,
        activity_type
)
,act2 AS (
SELECT
        client_uuid AS user_uuid,
        CASE WHEN client_nationality = 'China' THEN 'CN'
        WHEN client_nationality = 'Taiwan China' THEN 'TW'
        WHEN client_nationality = 'Hong Kong,China' THEN 'HK'
        WHEN client_nationality = 'Macao, China' THEN 'MO'
        END AS user_nationality,
        client_type AS user_type,
        case when activity_type = 'activity 1' then 'activity 2' else 'activity 2' end as activity_type ,
        SUM(crm_deposit) AS crm_deposit,
        SUM(crm_withdrawal) AS crm_withdrawal,
        SUM(crm_net_deposit) AS crm_net_deposit,
        SUM(mt_net_deposit) AS mt_net_deposit,
        SUM(qualified_trading_volume) AS trading_lots
FROM
        <db table 2>
GROUP BY
        client_uuid,
        CASE WHEN client_nationality = 'China' THEN 'CN'
        WHEN client_nationality = 'Taiwan China' THEN 'TW'
        WHEN client_nationality = 'Hong Kong,China' THEN 'HK'
        WHEN client_nationality = 'Macao, China' THEN 'MO'
        END,
        client_type,
        CASE WHEN activity_type = 'activity 1' THEN 'activity 2' ELSE 'activity 2' END
)
,spring_event as (
SELECT
        user_uuid,
        user_nationality,
        user_type,
        activity_type,
        crm_deposit ,
        crm_withdrawal,
        crm_net_deposit ,
        mt_net_deposit ,
        trading_lots
FROM
        act1
UNION SELECT
        user_uuid,
        user_nationality,
        user_type,
        activity_type,
        crm_deposit ,
        crm_withdrawal,
        crm_net_deposit ,
        mt_net_deposit ,
        trading_lots
FROM
        act2
)
,full_ AS (
SELECT
            CASE WHEN activity_type = 'Activity 1' THEN 'Name 1'
            WHEN activity_type = 'Activity 2' THEN 'Name 2'
            END AS campaign_full_name,
            '2024' AS campaign_year,
            CASE WHEN activity_type = 'Activity 1' THEN 'N1'
            WHEN activity_type = 'Activity 2' THEN 'N2'
            END AS campaign_name,
            '(CN)' AS campaign_nationality,
            'Client' AS campaign_target,
            CASE WHEN se.activity_type = 'Activity 1' THEN '11'
            ELSE '31'
            END AS campaign_duration,
            CASE WHEN se.activity_type = 'Activity 1' THEN '2000/03/01 - 2000/03/11'
            ELSE '2000/03/01 - 2000/03/31'
            END AS campaign_period,
            se.*,
            CASE WHEN (se.crm_net_deposit >= 12000) AND (se.trading_lots >= 30) AND (se.activity_type = 'Activity 1') THEN 120
            WHEN (se.crm_net_deposit >= 5000) AND (se.trading_lots >= 10) AND (se.activity_type = 'Activity 1') THEN 36
            ELSE 0
            END AS reward,
            NULL AS redeemed_reward,
            CASE WHEN se.crm_deposit > 0 OR se.crm_withdrawal < 0 OR se.trading_lots > 0 THEN 1
            ELSE 0
            END AS active_client,
            0 AS active_ib,
            null AS eligible_user,
            NULL AS redeemed_user
FROM
            spring_event se
)
,rebate1 AS (
  -- select rebate code 1
)
,rebate2 AS (
  -- select rebate code 2
)
,rebates AS (
SELECT
            client_uuid,
            SUM(rebate_usd) AS rebate_usd,
            activity
FROM
            rebate1
GROUP BY
            client_uuid,
            activity
UNION SELECT
            client_uuid,
            SUM(rebate_usd),
            activity
FROM
            rebate2
GROUP BY
            client_uuid,
            activity
)
,points1 AS (
  -- select points 1
)
,points2 AS (
  -- select points 2
)
,points AS (
SELECT      customer_uuid,
            points_earned,
            activity
FROM
            points1
UNION SELECT
            customer_uuid,
            points_earned,
            activity
FROM
            points2
)
,campaign_part AS (
SELECT      `USER ID`,
            ID,
            Name,
            Reward
FROM
            <table split>
WHERE
            ID = '10' OR ID = '20'
)
,rebate_table AS (
SELECT      f.campaign_full_name,
            campaign_year,
            campaign_name,
            campaign_nationality,
            campaign_target,
            campaign_duration,
            campaign_period,
            user_uuid,
            user_nationality,
            user_type,
            activity_type,
            crm_deposit,
            crm_withdrawal,
            crm_net_deposit,
            mt_net_deposit,
            trading_lots,
            f.reward,
            rebates.rebate_usd,
            points.points_earned,
            campaign_part.Reward AS redeemed_reward,
            active_client,
            active_ib,
            case when f.reward > 0 then 1 else 0 end as eligible_user,
            case when campaign_part.Reward > 0 then 1 else 0 end AS redeemed_user
FROM
            full_ f
LEFT JOIN
            rebates ON f.user_uuid = rebates.client_uuid
            AND f.activity_type = rebates.activity
LEFT JOIN
            points ON f.user_uuid = points.customer_uuid
            AND f.activity_type = points.activity
LEFT JOIN
            campaign_part ON f.user_uuid = campaign_part.`User ID`
            AND f.campaign_full_name = campaign_part.Name)
SELECT
            *
FROM
            rebate_table;
