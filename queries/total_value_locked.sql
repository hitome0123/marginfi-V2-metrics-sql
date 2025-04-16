
-- =============================================
-- MarginFi V2 - Total Value Locked (TVL) Calculation
-- =============================================

-- 1. Extract decoded instructions related to deposits and withdrawals from MarginFi
WITH marginfi_actions AS (
  SELECT 
    a.event_type,  -- Event type: Deposit or Withdrawal
    a.decoded_instruction:"args":"amount"::FLOAT AS raw_amount,  -- Raw amount (in smallest units)
    acc.value:"pubkey"::STRING AS account_address  -- The account address used by the user
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi v2 contract address
    AND a.event_type IN ('lendingAccountDeposit', 'lendingAccountWithdraw')  -- Filter deposit and withdrawal instructions
    AND acc.value:"name"::STRING = 'signerTokenAccount'  -- Signer's token ATA
),

-- 2. Retrieve the corresponding token's mint address (i.e., token type) from the token account
token_info AS (
  SELECT DISTINCT account_address, mint
  FROM solana.core.fact_token_balances 
),

-- 3. Build token metadata: prioritize ez_asset_metadata, then infer using symbol, and finally use fallback manual patches
asset_metadata AS (
  SELECT 
    ezm.token_address,
    ezm.symbol,
    COALESCE(
      ezm.decimals,  -- Prioritize the official decimals
      -- If missing, try symbol-based inference
      CASE 
        WHEN LOWER(ezm.symbol) LIKE '%usdc' THEN 6
        WHEN LOWER(ezm.symbol) LIKE '%usdt' THEN 6
        WHEN LOWER(ezm.symbol) LIKE '%usd%' THEN 6
        WHEN LOWER(ezm.symbol) LIKE '%pump%' THEN 6
        WHEN LOWER(ezm.token_address) LIKE '%pump' THEN 6
        WHEN ezm.token_address IN( 'ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY' 
                                   ,'CTJf74cTo3cw8acFP1YXF3QpsQUUBGBjh2k2e8xsZ6UL'
                                   ,'3S8qX1MsMqRbiwKg2cQyx7nis1oHMgaCuc9c4VfvVdPN') THEN 6
        WHEN LOWER(ezm.symbol) LIKE '%sol' THEN 9
        WHEN ezm.token_address IN( 'oreoN2tQbHXVaZsr3pf66A48miqcBXCDJozganhEJgz' 
                                   ,'HRw8mqK8N3ASKFKJGMJpy4FodwR3GKvCFKPDQNqUNuEP'
                                   ,'CLoUDKc4Ane7HeQcPpE3YHnznRxhMimJ4MyaUqyHFzAu'
                                   ,'8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5') THEN 9
        ELSE NULL
      END
    ) AS decimals
  FROM solana.price.ez_asset_metadata ezm
),

-- 4. Retrieve the main price source for the token (latest hour) → ez_prices_hourly
main_prices AS (
  SELECT token_address, price
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana'
    AND hour = (SELECT MAX(hour) FROM solana.price.ez_prices_hourly WHERE blockchain = 'solana')
),

-- 5. Retrieve backup price source → fact_prices_ohlc_hourly (original price close field)
backup_prices AS (
  SELECT dm.token_address, f.close AS price
  FROM solana.price.fact_prices_ohlc_hourly f
  INNER JOIN solana.price.dim_asset_metadata dm
    ON f.asset_id = dm.asset_id
  WHERE f.hour = (SELECT MAX(hour) FROM solana.price.fact_prices_ohlc_hourly)
    AND dm.blockchain = 'solana'
),

-- 6. Merge main and backup price sources (prefer using main price)
final_prices AS (
  SELECT 
    mp.token_address, 
    COALESCE(mp.price, bp.price) AS price  -- Prefer using main price (main_prices)
  FROM main_prices mp
  LEFT JOIN backup_prices bp 
    ON mp.token_address = bp.token_address  -- If main price is NULL, use backup price
),

-- 7. Core aggregation logic: Summing the net deposit and withdrawal amount for each token (raw + USD)
aggregated AS (
  SELECT 
    ti.mint,         -- Token mint address (unique identifier)
    am.symbol,       -- Token symbol
    SUM(
      CASE 
        WHEN ma.event_type = 'lendingAccountDeposit' THEN ma.raw_amount
        WHEN ma.event_type = 'lendingAccountWithdraw' THEN -ma.raw_amount
      END
    ) AS net_amount_raw,  -- Net deposit/withdraw in raw units (smallest unit)
    SUM(
      CASE 
        WHEN ma.event_type = 'lendingAccountDeposit' THEN (ma.raw_amount / POWER(10, am.decimals)) * fp.price
        WHEN ma.event_type = 'lendingAccountWithdraw' THEN (-ma.raw_amount / POWER(10, am.decimals)) * fp.price
      END
    ) AS net_amount_usd  -- Net deposit/withdraw in USD
  FROM marginfi_actions ma
  INNER JOIN token_info ti ON ma.account_address = ti.account_address
  INNER JOIN asset_metadata am ON ti.mint = am.token_address
  INNER JOIN final_prices fp ON am.token_address = fp.token_address
  GROUP BY ti.mint, am.symbol
)

-- 8. Final output
SELECT 
  SUM(net_amount_usd) AS TVL  -- Calculate total TVL
FROM aggregated;

