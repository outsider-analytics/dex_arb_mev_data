CREATE TEMP FUNCTION hexToInt(hex STRING)
Returns STRING   
LANGUAGE js AS r"""

    if (hex.length == 2) {
        yourNumber = 0
    } else {
        yourNumber = BigInt(hex)
    };

    if (yourNumber > 2 ** 256 / 2) {
        output = yourNumber - BigInt(2 ** 256)
    } else {
        output = yourNumber
    };


    return output;
""";

-- Get all transfers
WITH transfers as (
    -- Look for token transfers out from logs and set them to the proper acct
    SELECT 
        1 as location,
        CONCAT("0x",RIGHT(topics[SAFE_OFFSET(2)],40)) as counter_party,
        CONCAT("0x",RIGHT(topics[SAFE_OFFSET(1)],40)) as acct_address,
        -CAST(hexToInt(data) as float64) as amt,
        block_number,
        transaction_hash,
        address as token_address,
        CASE WHEN topics[SAFE_OFFSET(3)] IS NULL AND topics[SAFE_OFFSET(1)] IS NOT NUll THEN 0
             ELSE 1 END AS nft_check,
        1 as num_outs,
        CASE WHEN CONCAT("0x",RIGHT(topics[SAFE_OFFSET(1)],40)) = "0x0000000000000000000000000000000000000000" THEN 1
        ELSE 0 END as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.post_7_31_logs` l
    WHERE topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
    OR topics[SAFE_OFFSET(0)] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
    UNION ALL
    -- Look for token transfers in from logs and set them to the proper acct
    SELECT 
        2 as location,
        CONCAT("0x",RIGHT(topics[SAFE_OFFSET(1)],40)) as counter_party,
        CONCAT("0x",RIGHT(topics[SAFE_OFFSET(2)],40)) as acct_address,
        CAST(hexToInt(data) as float64) as amt,
        block_number,
        transaction_hash,
        address as token_address,
        CASE WHEN topics[SAFE_OFFSET(3)] IS NULL AND topics[SAFE_OFFSET(1)] IS NOT NULL THEN 0
             ELSE 1 END AS nft_check,
        0 as num_outs,
        CASE WHEN CONCAT("0x",RIGHT(topics[SAFE_OFFSET(1)],40)) = "0x0000000000000000000000000000000000000000" THEN 1
        ELSE 0 END as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.post_7_31_logs` l
    WHERE topics[SAFE_OFFSET(0)] = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" 
    OR topics[SAFE_OFFSET(0)] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
    UNION ALL
    -- add an extra transaction for Weth which accounts for weth deposits
    SELECT
        3 as location,
        to_address as counter_party,
        from_address as acct_address,
        value as amt,
        block_number,
        transaction_hash,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
        0 as nft_check,
        0 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    AND to_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    -- add an extra transactoin for Weth which accounts for weth deposits
    SELECT
        4 as location,
        from_address as counter_party,
        to_address as acct_address,
        -value as amt,
        block_number,
        transaction_hash,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
        0 as nft_check,
        1 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    AND to_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
-- add an extra transactoin for Weth which accounts for weth withdrawals
    SELECT
        5 as location,
        from_address as counter_party,
        to_address as acct_address,
        -value as amt,
        block_number,
        transaction_hash,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
        0 as nft_check,
        1 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    AND from_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    -- add an extra transactoin for Weth which accounts for weth withdrawals
    SELECT
        6 as location,
        to_address as counter_party,
        from_address as acct_address,
        value as amt,
        block_number,
        transaction_hash,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
        0 as nft_check,
        1 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    AND from_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    -- SELECT
    --     CONCAT("0x",SUBSTR(input, 35, 64)) as acct_address,
    --     CAST(hexToInt(CONCAT("0x",RIGHT(input,64))) as float64) as amt,
    --     block_number,
    --     transaction_hash,
    --     to_address as token_address,
    --     0 as nft_check,
    --     0 as num_outs,
    --     CASE WHEN from_address = "0x0000000000000000000000000000000000000000" THEN 1
    --     ELSE 0 END as null_check,
    --     0 as amm_check
    -- FROM `starlit-sandbox-349500.sample_data.1daytrace`
    -- WHERE left(input,10) = '0xa9059cbb'
    -- AND length(input) = 138
    -- UNION ALL
    -- SELECT
    --     from_address as acct_address,
    --     -CAST(hexToInt(CONCAT("0x",RIGHT(input,64))) as float64) as amt,
    --     block_number,
    --     transaction_hash,
    --     to_address as token_address,
    --     0 as nft_check,
    --     1 as num_outs,
    --     CASE WHEN to_address = "0x0000000000000000000000000000000000000000" THEN 1
    --     ELSE 0 END as null_check,
    --     0 as amm_check
    -- FROM `starlit-sandbox-349500.sample_data.1daytrace`
    -- WHERE left(input,10) = '0xa9059cbb'
    -- AND length(input) = 138
    -- UNION ALL
    -- Check all transfers of ether within traces
    SELECT
        7 as location,
        from_address as counter_party,
        to_address as acct_address,
        value as amt,
        block_number,
        transaction_hash,
        "ether" as token_address,
        0 as nft_check,
        0 as num_outs,
        CASE WHEN from_address = "0x0000000000000000000000000000000000000000" THEN 1
        ELSE 0 END as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    UNION ALL
    -- Check all transfers of ether within traces
    SELECT
        8 as location,
        to_address as counter_party,
        from_address as acct_address,
        -value as amt,
        block_number,
        transaction_hash,
        "ether" as token_address,
        0 as nft_check,
        1 as num_outs,
        CASE WHEN to_address = "0x0000000000000000000000000000000000000000" THEN 1
        ELSE 0 END as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.7_31_traces`
    WHERE value > 0
    AND status = 1
    UNION ALL
    -- Do the Gas Calculation
    SELECT
        9 as location,
        "gas_costs" as counter_party,
        from_address as acct_address,
        SAFE_MULTIPLY(-gas_price, receipt_gas_used) as amt,
        block_number,
        t.hash,
        "ether" as token_address,
        0 as nft_check,
        0 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        SAFE_MULTIPLY(gas_price, receipt_gas_used) as txn_gas_costs
    FROM `starlit-sandbox-349500.Data.post_7_31_txs` t
    WHERE receipt_gas_used > 0
    UNION ALL
    -- check and see if an account is an AMM
    SELECT 
        10 as location,
        "amm_check" as counter_party,
        address as acct_address,
        0 as amt, 0.0000267264
        block_number,
        transaction_hash,
        address as token_address,
        0 as nft_check,
        0 as num_outs,
        0 as null_check,
        1 as amm_check,
        block_timestamp,
        0 as txn_gas_costs    
    FROM `starlit-sandbox-349500.Data.post_7_31_logs` l
    WHERE topics[SAFE_OFFSET(0)] = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
    UNION ALL
    -- I'm not sure what this is, maybe its an ether transfer by a different code?
    SELECT 
        11 as location,
        to_address as counter_party,
        from_address as acct_address,
        -CAST(hexToInt(CONCAT("0x",SUBSTR(input, 75, 64))) AS FLOAT64) as amt,
        block_number,
        transaction_hash,
        "ether" as token_address,
        0 as nft_check,
        1 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs      
    FROM `starlit-sandbox-349500.Data.pre_merge_traces` l
    WHERE left(input,10) = '0xf5cc61a0'
    AND status = 1
    UNION ALL
    --I'm not sure, see above
    SELECT 
        12,
        from_address as counter_party,
        to_address as acct_address,
        CAST(hexToInt(CONCAT("0x",SUBSTR(input, 75, 64))) AS FLOAT64) as amt,
        block_number,
        transaction_hash,
        "ether" as token_address,
        0 as nft_check,
        0 as num_outs,
        0 as null_check,
        0 as amm_check,
        block_timestamp,
        0 as txn_gas_costs  
    FROM `starlit-sandbox-349500.Data.7_31_traces` t
    WHERE left(input,10) = '0xf5cc61a0'
    AND status = 1
),

grouped_by_tx_token as (
    SELECT
        acct_address,
        block_number,
        transaction_hash,
        token_address,
        sum(amt) as net_flows,
        sum(nft_check) as num_nft_trs,
        sum(num_outs) as num_outs,
        sum(null_check) as null_check,
        sum(amm_check) as amm_check,
        count(*) as num_transfers,
        block_timestamp,
        sum(transfers.txn_gas_costs) as tx_gas_costs
    FROM transfers
    GROUP BY acct_address, block_number, transaction_hash, transfers.token_address, block_timestamp
),

windows as (
    SELECT
        CASE WHEN g.acct_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                AND token_address = 'ether'
             THEN from_address
             ELSE g.acct_address END as acct_address,
        -- g.acct_address,
        g.block_number,
        g.transaction_hash,
        g.token_address,
        g.num_outs,
        g.net_flows,
        MIN(net_flows) OVER (PARTITION BY acct_address, transaction_hash) as neg_check,
        SUM(num_nft_trs) OVER (PARTITION BY transaction_hash) as num_nft_trs_per_tx,
        SUM(null_check) OVER (PARTITION BY transaction_hash) as num_null_check,
        SUM(amm_check) OVER (PARTITION BY acct_address, transaction_hash) as sum_amm_check,
        SUM(num_transfers) OVER (PARTITION BY transaction_hash) as num_transfers,
        from_address,
        t.input,
        SAFE_MULTIPLY(t.gas_price,t.receipt_gas_used) as gas_costs,
        t.value,
        t.block_timestamp,
        MAX(g.tx_gas_costs) OVER (PARTITION BY transaction_hash) as tx_gas_costs,
        t.gas_price,
        t.receipt_gas_used
        -- SUM(CASE WHEN g.acct_address = t.from_address AND g.net_flows < 0 THEN 1
        --      ELSE 0 END) OVER (PARTITION BY transaction_hash) as tx_from_address 
    FROM grouped_by_tx_token g
    LEFT JOIN `starlit-sandbox-349500.Data.post_7_31_txs` t
    ON t.hash = g.transaction_hash
    WHERE t.receipt_status = 1
    AND LEFT(t.input,10) != '0x3ccfd60b' --straight withdrawal
    AND LEFT(t.input,10) != '0x0cded5f0' --migratePool
),
grouped_again as (
    SELECT 
        acct_address,
        block_number,
        transaction_hash,
        token_address,
        sum(num_outs) as num_outs,
        sum(net_flows) as net_flows,
        sum(neg_check) as neg_check,
        num_nft_trs_per_tx,
        num_null_check,
        sum_amm_check,
        from_address,
        input,
        num_transfers,
        gas_costs,
        value,
        block_timestamp,
        tx_gas_costs,
        gas_price,
        receipt_gas_used,
    FROM windows
    GROUP BY acct_address, block_number, transaction_hash, token_address,
        num_nft_trs_per_tx, num_null_check, sum_amm_check, from_address, input, num_transfers, gas_costs, value, block_timestamp, tx_gas_costs, gas_price, receipt_gas_used
    ),


no_neg_from as (
    SELECT
        *,    
        SUM(CASE WHEN acct_address = from_address AND net_flows < (-gas_costs) THEN 1
            ELSE 0 END) OVER (PARTITION BY transaction_hash) as tx_from_address, 
        SUM(CASE WHEN acct_address = '0x11546014529f85f2c0873cc8c2d5b45cdf98edd3' THEN 1
            ELSE 0 END) OVER (PARTITION BY transaction_hash) as bad_contract -- anything interacting with this contract is not MEV but doing something with Uniswap refunds 
    FROM grouped_again
)

SELECT 
    acct_address,
    transaction_hash,
    token_address,
    net_flows/1e18 as net_flows,
    from_address,
    num_outs,
    neg_check,
    num_transfers,
    block_number,
    block_timestamp,
    tx_gas_costs,
    gas_price,
    receipt_gas_used
    -- min(transaction_hash) as transaction_hash,
    -- sum(net_flows/10e17) as net_flows,
    -- min(token_address) as token_address,
    -- count(*) as count_txs
FROM  no_neg_from
WHERE num_nft_trs_per_tx = 0
AND neg_check >= 0
-- AND num_outs > 0
AND net_flows > 10e10
AND num_null_check = 0
AND sum_amm_check = 0
AND tx_from_address = 0
AND bad_contract = 0
-- AND num_transfers > 6
AND (token_address = 'ether' or token_address = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'))
AND LEFT(input,10) != '0xd96d6594' --reinvest() (amms)
AND LEFT(input,10) != '0x7d7c2a1c' --rebalance() (amms)
AND LEFT(input,10) != '0xc84ba764' -- 1 inch
AND LEFT(input,10) != '0x6a761202' -- amm rebalancer
AND LEFT(input,10) != '0x47884ac3' --some kind of router 
AND LEFT(input,10) != '0x132db5ea' -- maybe MEV but i don't like it 
AND LEFT(input,10) != '0x6bf0f4a1' --roll() some dice game
AND acct_address != '0x4ff0dec5f9a763aa1e5c2a962aa6f4edfee4f9ea'
AND acct_address != '0xc186fa914353c44b2e33ebe05f21846f1048beda'
AND acct_address != '0x288931fa76d7b0482f0fd0bca9a50bf0d22b9fef' --1-inch
AND acct_address != '0xf2f400c138f9fb900576263af0bc7fcde2b1b8a8' --1-inch v4
AND acct_address != '0x4d73adb72bc3dd368966edd0f0b2148401a178e2' --stargate/ultralightnode v2
AND acct_address != '0x9008d19f58aabd9ed0d60971565aa8510560ab41' --cowswap GPv2 Settlement
AND acct_address != '0x4a14347083b80e5216ca31350a2d21702ac3650d' --Not sure but takes a cut of AMM transfers
AND acct_address != '0xfd6c2d2499b1331101726a8ac68ccc9da3fab54f' --Part of TokenIon DEx
AND acct_address != '0x76f4eed9fe41262669d0250b2a97db79712ad855' --Odos Router
AND acct_address != '0x9fb131efbac23b735d7764ab12f9e52cc68401ca' --Kyber Network Fee Handler
AND acct_address != '0x9981cd5cd2e7ad5dda4b7896e22688a32a500b27' -- Uni-Booster
AND acct_address != '0xa7053782dc3523d2c82b439acf3f9344fb47b97f' -- Uni-Booster
AND acct_address != '0x58b4f284fc53bbf349c5168d3f82323189b9a1c9' -- Velox Router
AND acct_address != '0xc4e0302a9d840db3065210f65aad026e9d695c13' -- Wierd contract probably not MEV
AND acct_address != '0xa1006d0051a35b0000f961a8000000009ea8d2db' -- Wierd contract probably not MEV 
AND acct_address != '0x1b72d900ee50ca200097003100001f30ae00495b' -- Wierd contract probably not MEV 
AND acct_address != '0x030ba81f1c18d280636f32af80b9aad02cf0854e' -- AAVE Tracking wierd
-- AND transaction_hash = '0xa2044980f7880454a770db3e725596975ed6be26ef390b306dbc9d6a6a7c9ebe' 0x07fe92794bf5a8af4d2523d390bb5c2e1ed856e5e14ca394d6d50f076002a1d1
-- GROUP BY acct_address
ORDER BY net_flows

