WITH miner_MEV as (
  SELECT
    *,
    0 as miner
  FROM `starlit-sandbox-349500.Data.MEV_FINDER_RESULTS_2` 
  UNION ALL  
  SELECT
    MAX(b.miner) as acct_address,
    transaction_hash,
    "ether" as token_address,
    MAX((gas_price - b.base_fee_per_gas) * (receipt_gas_used/1e18)) as net_flows,
    MAX(from_address),
    1 as num_outs, --I'll fudge this to keep it in
    0 neg_check,
    AVG(num_transfers),
    MAX(block_number),
    MAX(block_timestamp),
    MAX(tx_gas_costs),
    MAX(gas_price),
    MAX(receipt_gas_used),
    1 as miner
  FROM `starlit-sandbox-349500.Data.MEV_FINDER_RESULTS_2` r
  LEFT JOIN `bigquery-public-data.crypto_ethereum.blocks` b
    ON b.number = r.block_number
  WHERE r.acct_address != '0x030ba81f1c18d280636f32af80b9aad02cf0854e'
  GROUP BY transaction_hash
),

separate_miner as (
  SELECT 
    *,
    SUM(miner) OVER (PARTITION BY acct_address, transaction_hash)
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
  AND num_outs > 0
  ORDER BY net_flows desc)

SELECT 
  pre_merge,
  day_diff,
  COUNT(DISTINCT acct_address) AS MEV_Distinct_Participants,
  COUNT(*) AS num_MEV,
  SUM(net_flows) AS total_MEV,
  SUM(net_flows)/COUNT(*) as MEV_per_opportunity
FROM merge_check
GROUP BY pre_merge, day_diff
ORDER BY day_diff, pre_merge
