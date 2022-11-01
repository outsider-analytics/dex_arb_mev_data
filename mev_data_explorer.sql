



WITH miner_MEV as (
  SELECT
    *,
    0 as miner
  FROM `starlit-sandbox-349500.Data.MEV_FINDER_RESULTS_2` 
  UNION ALL  
  SELECT
    b.miner as acct_address,
    transaction_hash,
    "ether" as token_address,
    (gas_price - b.base_fee_per_gas) * (receipt_gas_used/1e18) as net_flows,
    from_address,
    0 as num_outs,
    0 neg_check,
    num_transfers,
    block_number,
    block_timestamp,
    tx_gas_costs,
    gas_price,
    receipt_gas_used,
    1 as miner
  FROM `starlit-sandbox-349500.Data.MEV_FINDER_RESULTS_2` r
  LEFT JOIN `bigquery-public-data.crypto_ethereum.blocks` b
    ON b.number = r.block_number
  WHERE r.acct_address != '0x030ba81f1c18d280636f32af80b9aad02cf0854e'
),

separate_miner as (
  SELECT 
    *,
    MAX(miner) OVER (PARTITION BY acct_address, transaction_hash) as miner_check
  FROM miner_MEV
),


merge_check as (
  SELECT 
    *,
    CASE WHEN block_number < 15537393 THEN "pre"
          ELSE "post" END AS pre_merge,
    CASE WHEN block_number < 15537393 
          THEN trunc(DATE_DIFF(DATE(block_timestamp), DATE '2022-8-1', DAY))
          ELSE trunc(DATE_DIFF(timestamp(block_timestamp), timestamp('2022-9-15 06:42:42'), DAY))
          END AS day_diff
  FROM separate_miner
  WHERE acct_address != '0x030ba81f1c18d280636f32af80b9aad02cf0854e'
  -- AND acct_address != '0x05656db19ec9ff8dfb437475b3d76ca9a29e968f'
  -- GROUP BY acct_address, token_address
  ORDER BY net_flows desc)

-- FOR pre/post merge chart
-- SELECT 
--   pre_merge,
--   day_diff,
--   COUNT(DISTINCT acct_address) AS MEV_Distinct_Participants,
--   COUNT(*) AS num_MEV,
--   SUM(net_flows) AS total_MEV,
--   SUM(net_flows)/COUNT(*) as MEV_per_opportunity
-- FROM merge_check
-- GROUP BY pre_merge, day_diff
-- ORDER BY day_diff, pre_merge

--To View Miner Share of MEV
SELECT
  pre_merge,
  miner_check,
  SUM(net_flows)
FROM merge_check
GROUP BY pre_merge, miner_check
