--------------------------------------------------------------------------------
-- 1. Extract Deposit operations related to BankLiquidityVault to calculate Deposit Outstanding (unpaid borrow balance)
-- Source: solana.core.fact_decoded_instructions
-- Note: This module takes a "protocol perspective". BankLiquidityVault is the core account of protocol asset flows.
-- By tracking this account, we can accurately trace deposit outflows at the protocol level for Outstanding calculation.
--------------------------------------------------------------------------------
WITH deposit_actions AS (
  SELECT DISTINCT
    a.block_timestamp, -- Event type: only lendingAccountBorrow, used to filter relevant events
    a.decoded_instruction:"args":"amount"::FLOAT AS deposit_raw_amount, -- deposit amount (not normalized)
    acc.value:"pubkey"::STRING AS deposit_token_address -- Internal protocol account for deposit asset outflow (BankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc -- Flatten accounts to extract bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingAccountDeposit' -- deposit event
    AND acc.value:"name"::STRING = 'bankLiquidityVault'
    -- From protocol perspective, bankLiquidityVault represents the core account for asset inflow/outflow.
    -- It is used to measure deposit activity at the protocol level.
    -- AND a.BLOCK_TIMESTAMP >= CURRENT_DATE - INTERVAL '30 days' -- Only keep data from the last 30 days
),
--------------------------------------------------------------------------------
-- 2. Extract Withdraw operations related to BankLiquidityVault for Deposit Outstanding calculation
-- Source: solana.core.fact_decoded_instructions
-- When a user withdraws, funds flow out from this vault. Therefore, tracking these outflows reflects
-- the protocol’s reduction in user deposits (i.e., outstanding deposits).
-------------------------------------------------------------------------------- 
withdraw_actions AS (
  SELECT DISTINCT
    a.decoded_instruction:"args":"amount"::FLOAT AS withdraw_raw_amount, -- Withdraw amount (not normalized)
    acc.value:"pubkey"::STRING AS withdraw_token_address  -- Token account associated with the withdrawal (BankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc -- Flatten accounts array from decoded instruction
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
    AND a.event_type = 'lendingAccountWithdraw' -- Only include withdraw events
    AND acc.value:"name"::STRING = 'bankLiquidityVault' -- Withdraw source must be BankLiquidityVault (protocol vault)
),

--------------------------------------------------------------------------------
-- 3. Map Token Account to Mint for identifying token types
-- Source: solana.core.fact_token_balances
-- This table maps each token account (SPL ATA) to its corresponding mint address
-- Note: Token accounts (ATA) are linked to their mint addresses for metadata lookup
--------------------------------------------------------------------------------

token_info AS (
  SELECT DISTINCT account_address, -- SPL Token Account (ATA)
  mint                             -- Associated token mint (unique identifier)
  FROM solana.core.fact_token_balances tf
  WHERE EXISTS (
  SELECT 1 
  FROM deposit_actions da
  WHERE da.deposit_token_address = tf.account_address
)
),
  
--------------------------------------------------------------------------------
-- 4. Build asset metadata: get symbol and decimals for each token
-- Priority:
--   (1) Use official decimals from ez_asset_metadata if available
--   (2) Fallback: infer decimals from symbol patterns
--   (3) If still missing, apply hardcoded values for verified token addresses
-- Notes:
--   - Most LP tokens use the same decimals as their base token (e.g., USDC-LP, ETH-LP)
--   - PumpFun meme tokens default to 6
--   - Hardcoded entries were manually verified
--------------------------------------------------------------------------------

asset_metadata AS (
  SELECT 
    ezm.token_address, -- Token address
    ezm.symbol,        -- Token symbol (used for inferring decimal precision if missing)
    COALESCE(
      ezm.decimals,  -- Prioritize the official decimals

    -- If missing, infer decimals from symbol patterns: 
    -- LP tokens generally share the same decimal precision as their native tokens (e.g., USDC-LP uses the same decimals as USDC)
      CASE 
        WHEN LOWER(ezm.symbol) LIKE '%usd%' THEN 6  -- Stablecoins & LPs (e.g., LP-USDC, cUSDC): use 6 decimals
        WHEN LOWER(ezm.symbol) LIKE '%eth%' THEN 8  -- ETH-related tokens: use 8 decimals
        WHEN LOWER(ezm.symbol) LIKE '%sol' THEN 9   -- SOL-related tokens: use 9 decimals

        -- PumpFun tokens: default to 6
        WHEN LOWER(ezm.symbol) LIKE '%pump%' THEN 6 
        WHEN LOWER(ezm.token_address) LIKE '%pump' THEN 6

        -- Verified fallback decimals for known tokens
        WHEN ezm.token_address IN( 'ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY' 
                                   ,'CTJf74cTo3cw8acFP1YXF3QpsQUUBGBjh2k2e8xsZ6UL'
                                   ,'3S8qX1MsMqRbiwKg2cQyx7nis1oHMgaCuc9c4VfvVdPN') THEN 6
        WHEN ezm.token_address IN( 'oreoN2tQbHXVaZsr3pf66A48miqcBXCDJozganhEJgz' 
                                   ,'HRw8mqK8N3ASKFKJGMJpy4FodwR3GKvCFKPDQNqUNuEP'
                                   ,'CLoUDKc4Ane7HeQcPpE3YHnznRxhMimJ4MyaUqyHFzAu'
                                   ,'8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5') THEN 9
        ELSE NULL
      END
    ) AS decimals 
  FROM solana.price.ez_asset_metadata ezm
),

--------------------------------------------------------------------------------
-- 5. Retrieve latest price for each token (used in Deposit Outstanding calculation)
-- Only the latest hour’s price is needed, not historical hourly prices
-- Priority:
--   - Primary: ez_prices_hourly
--   - Fallback: fact_prices_ohlc_hourly (close)
-- Explanation: The latest price is used for Deposit Outstanding to reflect the current market value of assets that are still locked in the protocol. 
--- Since Deposit Outstanding represents the current amount of funds that users have deposited but not yet withdrawn, 
--- it is essential to use the most up-to-date price to ensure that the calculation accurately reflects the current value of those assets.
--------------------------------------------------------------------------------
-- ① Primary price source (Only fetch the most recent hour’s data for each token)
-- Source: mainstream on-chain price table “ez_prices_hourly”
lp_main_prices AS (
  SELECT 
    token_address,     
    price              
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana' 
  AND hour = (  -- Only select the most recent hour
      SELECT MAX(hour) 
      FROM solana.price.ez_prices_hourly 
    )
),

-- ② Backup price source (OHLC closing price)
-- Source: “close” field from “fact_prices_ohlc_hourly” table
-- Used as a fallback when the main price source is missing
lp_backup_prices AS (
  SELECT *
  FROM (
    SELECT 
      dm.token_address,
      f.close AS price,
      ROW_NUMBER() OVER (PARTITION BY dm.token_address ORDER BY f.hour DESC) AS rn
    FROM solana.price.fact_prices_ohlc_hourly f
    INNER JOIN solana.price.dim_asset_metadata dm 
      ON f.asset_id = dm.asset_id
    WHERE dm.blockchain = 'solana'
  ) t
  WHERE rn = 1
),

-- ③ Merge primary and backup price sources
-- Prefer the primary price; fallback to backup price if primary is missing
lp_final_prices AS (
  SELECT 
    COALESCE(lm.token_address, lb.token_address) AS token_address,  -- Prefer token address from the primary price source
    COALESCE(lm.price, lb.price) AS price                           -- Fallback to backup price if missing
  FROM lp_main_prices lm  
  LEFT JOIN lp_backup_prices lb
    ON lm.token_address = lb.token_address
),
--------------------------------------------------------------------------------
-- 6. Compute Deposit Outstanding (remaining deposit balance in USD)
-- Source: protocol-perspective events from BankLiquidityVault
-- Logic: Outstanding = Deposit - withdraw, then multiply by latest token price
--------------------------------------------------------------------------------
outstanding_volume AS (
SELECT * FROM(
  SELECT
    ti.mint,
    am.symbol,

    -- Outstanding amount (normalized by decimals), calculated as: deposit - withdraw    
    SUM(
      (COALESCE(da.deposit_raw_amount, 0) - COALESCE(wa.withdraw_raw_amount, 0))/ POWER(10, am.decimals)
    ) AS outstanding_volume_raw,

    -- Outstanding deposit amount in USD = normalized token amount × latest price
    SUM(
      (
        COALESCE(da.deposit_raw_amount, 0) - COALESCE(wa.withdraw_raw_amount, 0)
      ) / POWER(10, am.decimals)                  -- Normalize using token decimals
        * COALESCE(lp.price, 0)                   -- Use latest price; fallback to 0 if missing
    ) AS outstanding_volume_usd
  FROM deposit_actions da
  LEFT JOIN token_info ti ON da.deposit_token_address = ti.account_address -- Map the token ATA to its corresponding mint address
  LEFT JOIN asset_metadata am ON ti.mint = am.token_address  -- Retrieve the token's decimals information
  LEFT JOIN lp_final_prices lp ON am.token_address = lp.token_address -- Map token to latest price
  LEFT JOIN withdraw_actions wa ON am.token_address = wa.withdraw_token_address -- Join withdrawals by token address
  GROUP BY ti.mint, am.symbol
)
WHERE 
  (
    --  Mainstream assets (USDC, SOL, mSOL, ETH, BTC)
    --     - Restrict net locked amount to less than 10 million (1e7) units
    --     - Prevent abnormal large balances from distorting TVL calculations
    (symbol IN ('usdc', 'sol', 'msol', 'eth', 'btc') AND outstanding_volume_raw < 1e7)
    OR
    --  Other minor assets (non-mainstream assets)
    --     - Allow net locked amount up to 0.1 billion (1e8) units
    --     - Accommodate the naturally large unit quantities of small-cap or meme tokens
    (symbol NOT IN ('usdc', 'sol', 'msol', 'eth', 'btc') AND outstanding_volume_raw < 1e8)
  
  --  Global filtering: Net locked amount must be greater than 0.1 units
  --     - Remove "dust" balances (extremely small holdings) to maintain accuracy
AND outstanding_volume_raw > 0.1
)
)

--------------------------------------------------------------------------------
-- 7. Deposit outstanding Summary
-- This section summarizes the Deposit_volume across all assets.
-- Final output values are converted and presented in millions (M USD) for better readability.
--------------------------------------------------------------------------------

SELECT
  'deposit_outstanding_usd(M)' AS metric,
  SUM(outstanding_volume_usd)/1e6  AS value 
FROM outstanding_volume;
