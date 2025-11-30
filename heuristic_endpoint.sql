WITH pool_liqs AS (
    SELECT
        dp.contract_address AS pool_address,
        verified_token,
        t.decimals AS verified_decimals,
        unverified_token,
        t2.symbol AS unverified_symbol,
        creator_address AS pool_deployer
    FROM ethereum.dex_pools dp
             CROSS JOIN unnest(token_addresses) AS verified_token
             JOIN ethereum.tokens t ON t.contract_address = verified_token AND t.verified = TRUE

             CROSS JOIN unnest(token_addresses) AS unverified_token
             JOIN ethereum.tokens t2 ON t2.contract_address = unverified_token AND t2.verified = FALSE
), action_count as (
    select pl.*,
    SUM(CASE WHEN dl.category = 'withdraw' and dl.token_address = pl.unverified_token and dl.sender_address = pl.pool_deployer THEN 1 ELSE 0 END) AS withdraw_count,
    SUM(CASE WHEN dl.category = 'swap' and dl.quantity > 0 and dl.sender_address = pl.pool_deployer and dl.token_address = pl.unverified_token and dl.sender_address = pl.pool_deployer THEN 1 ELSE 0 END) AS sell_count
    FROM pool_liqs pl
    JOIN  ethereum.dex_liquidity dl ON pl.pool_address = dl.contract_address
    WHERE dl.block_number < 20081867
    GROUP BY pl.pool_address, 
        pl.verified_token, 
        pl.verified_decimals, 
        pl.unverified_token, 
        pl.unverified_symbol, 
        pl.pool_deployer
), slow_rug as (
    select * from action_count where withdraw_count > 3 or sell_count > 3 and pl.burn > 0.99
)

select * from slow_rug
