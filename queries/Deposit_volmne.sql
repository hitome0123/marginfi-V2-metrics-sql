--------------------------------------------------------------------------------
-- 1. Extract Deposit actions related to BankLiquidityVault for computing deposit volume
-- Source: solana.core.fact_decoded_instructions
-- Note: This query takes a "protocol-level" perspective. The 'bankLiquidityVault'
--- account reflects the protocol's asset inflow, which can be used to measure user deposits into MarginFi.
--------------------------------------------------------------------------------
WITH deposit_actions AS (
  SELECT DISTINCT
    a.block_timestamp,   -- Deposit time, used for hourly price matching
    a.decoded_instruction:"args":"amount"::FLOAT AS deposit_raw_amount, -- Deposit amount (raw, not normalized by decimals)
    acc.value:"pubkey"::STRING AS deposit_token_address  -- Token account receiving the deposit (bankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc -- Flatten account list from decoded instruction
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingAccountDeposit' -- Filter for deposit events
    AND acc.value:"name"::STRING = 'bankLiquidityVault' -- Must be the bankLiquidityVault (protocol inflow)
    -- From protocol perspective, bankLiquidityVault represents the core account for asset inflow/outflow.
    -- It is used to measure deposit activity at the protocol level.
),
--------------------------------------------------------------------------------
-- 2. Map Token Account to Mint for identifying token types
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
-- 3. Build asset metadata: get symbol and decimals for each token
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
-- 4. Fetch hourly historical token prices for Borrow Volume calculation
-- Price sources:
--   Primary: solana.price.ez_prices_hourly
--   Backup:  solana.price.fact_prices_ohlc_hourly (uses OHLC close)
-- Strategy: match on token_address + hour; prefer primary price
-- Explanation: Borrow Volume is calculated using historical prices to reflect the actual borrowing cost 
--- at the time of the transaction. This ensures that the total borrow volume accurately matches the value 
--- agreed upon at the time of borrowing, without being affected by subsequent price fluctuations in the market.
--------------------------------------------------------------------------------
-- ① Primary price source (hourly granularity)
-- Source: mainstream on-chain price table “ez_prices_hourly”
  
hp_main_prices AS (
  SELECT 
    token_address,           
    price,                   
    hour                       -- Hourly timestamp used to match borrow event timing
  FROM solana.price.ez_prices_hourly
),

-- ② Backup price source (OHLC closing price)
-- Source: “close” field from “fact_prices_ohlc_hourly” table
-- Used as a fallback when the main price source is missing
hp_backup_prices AS (
  SELECT 
    dm.token_address,          
    f.close AS price,          -- ”close“ used as a supplementary price
    f.hour                     
  FROM solana.price.fact_prices_ohlc_hourly f
  JOIN solana.price.dim_asset_metadata dm 
    ON f.asset_id = dm.asset_id -- join on “asset_id” as the primary key
),
-- ③ Merge primary and backup price sources
-- Prefer the primary price; fallback to backup price if primary is missing
hp_final_prices AS (
  SELECT 
    mp.token_address,
    mp.hour,
    COALESCE(mp.price, bp.price) AS price  -- Prefer the primary price; fallback to backup price
  FROM hp_main_prices mp
  LEFT JOIN hp_backup_prices bp
    ON mp.token_address = bp.token_address  -- join on “token_address” as the primary key
   AND mp.hour = bp.hour                    -- join on “hour” as the primary key
),
--------------------------------------------------------------------------------
-- 5. Calculate Deposit Volume (normalized + USD)
-- Source: Protocol-perspective deposit events (bankLiquidityVault)
-- Core logic: For each borrow, normalize by token decimals and multiply by 
-- the hourly token price at the time of the deposit, then aggregate
--------------------------------------------------------------------------------
deposit_volume AS (
SELECT * FROM (
  SELECT
    ti.mint,
    am.symbol,

    -- Outstanding amount (normalized by decimals), calculated as: borrow - repay - liquidate    
    SUM(COALESCE(da.deposit_raw_amount, 0)/POWER(10, COALESCE(am.decimals, 0))) AS deposit_raw_amount, 

   -- Total deposit volume (USD) = Each deposit amount × corresponding hourly price, then aggregated
   SUM(

   -- Convert raw token amount to standardized unit using its decimals
      (COALESCE(da.deposit_raw_amount, 0) / POWER(10, COALESCE(am.decimals, 0))) 
      * COALESCE(hp.price, 0)     -- Use the corresponding hourly price; fallback to 0 if the price is missing
    ) AS deposit_volume_usd
  FROM deposit_actions da
  INNER JOIN token_info ti ON da.deposit_token_address = ti.account_address -- Map the token ATA to its corresponding mint address
  LEFT JOIN asset_metadata am ON ti.mint = am.token_address  -- Retrieve the token's decimals information 
  LEFT JOIN hp_final_prices hp
    ON am.token_address = hp.token_address 
    AND hp.hour = DATE_TRUNC('hour', da.BLOCK_TIMESTAMP) -- Precisely match the hourly price corresponding to the borrow timestamp
  GROUP BY ti.mint, am.symbol
)
WHERE 
  (
    --  Mainstream assets (USDC, SOL, mSOL, ETH, BTC)
    --     - Restrict net locked amount to less than 10 million (1e7) units
    --     - Prevent abnormal large balances from distorting TVL calculations
    (symbol IN ('USDC', 'SOL', 'mSOL', 'ETH', 'BTC') AND deposit_raw_amount < 1e7)
    OR
    --  Other minor assets (non-mainstream assets)
    --     - Allow net locked amount up to 0.1 billion (1e8) units
    --     - Accommodate the naturally large unit quantities of small-cap or meme tokens
    (symbol NOT IN ('USDC', 'SOL', 'mSOL', 'ETH', 'BTC') AND deposit_raw_amount < 1e8)
  )
  --  Global filtering: Net locked amount must be greater than 0.1 units
  --     - Remove "dust" balances (extremely small holdings) to maintain accuracy
AND deposit_raw_amount > 0.1
)
--------------------------------------------------------------------------------
-- 6. Deposit Volume Summary
-- This section summarizes the Deposit_volume across all assets.
-- Final output values are converted and presented in millions (M USD) for better readability.
--------------------------------------------------------------------------------

SELECT
  'deposit_volume_usd(M)' AS metric,
  SUM(deposit_volume_usd) / 1e6  AS value 
FROM deposit_volume;
