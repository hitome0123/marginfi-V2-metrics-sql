--------------------------------------------------------------------------------
-- 1. Extract MarginFi Borrow events for calculating Borrow Volume (total borrowed amount)
-- Source: solana.core.fact_decoded_instructions 
-- Source table containing decoded instruction data from all Solana programs (preferred for clarity and ease of parsing)
-- Note: This part takes a "user perspective", reflecting the actual asset outflow from MarginFi protocol
--------------------------------------------------------------------------------
WITH marginfi_borrows AS (
  SELECT 
    DATE_TRUNC('hour',a.BLOCK_TIMESTAMP) as borrow_time,  -- Borrow time, used for hourly price matching
    a.decoded_instruction:"args":"amount"::FLOAT AS raw_amount,  -- Borrowed amount (not normalized by decimals)
    acc.value:"pubkey"::STRING AS account_address  -- Token ATA from which the user receives the loan (destinationTokenAccount)
  FROM solana.core.fact_decoded_instructions a,
  LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc -- Flatten account list from decoded instruction
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingAccountBorrow'  -- Borrow event type
    AND acc.value:"name"::STRING = 'destinationTokenAccount'  -- Account name must be destinationTokenAccount
    -- destinationTokenAccount reflects the user's receiving account, capturing actual outflow from the protocol
    AND a.BLOCK_TIMESTAMP >= CURRENT_DATE - INTERVAL '30 days' -- Only keep data from the last 30 days
),
--------------------------------------------------------------------------------
-- 2. Map Token Account to Mint for identifying token types
-- Source: solana.core.fact_token_balances
-- This table maps each token account (SPL ATA) to its corresponding mint address
-- Note: Token accounts (ATA) are linked to their mint addresses for metadata lookup
--------------------------------------------------------------------------------
token_info AS (
  SELECT distinct
    account_address,  -- Token account (SPL ATA)
    mint              -- Associated token mint (unique identifier)
  FROM solana.core.fact_token_balances tf
WHERE EXISTS (
  SELECT 1 
  FROM marginfi_borrows mb
  WHERE mb.account_address = tf.account_address
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
      -- If missing, try symbol-based inference
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
--------------------------------------------------------------------------------
-- ① Primary price source (hourly granularity)
-- Source: mainstream on-chain price table “ez_prices_hourly”
  
hp_main_prices AS (
  SELECT 
    token_address,           
    price,                   
    hour                       -- Hourly timestamp used to match borrow event timing
  FROM solana.price.ez_prices_hourly
  WHERE hour >= CURRENT_DATE - INTERVAL '30 days'
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
    WHERE hour >= CURRENT_DATE - INTERVAL '30 days'
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
-- 5. Calculate Borrow Volume (total borrow amount in USD)
-- Source: User-perspective borrow events (destinationTokenAccount)
-- Core logic: For each borrow, normalize by token decimals and multiply by 
-- the hourly token price at the time of the borrow, then aggregate
--------------------------------------------------------------------------------
borrow_volume AS (
  SELECT
    ti.mint,                     -- Token mint address
    SUM(COALESCE(mb.raw_amount, 0)) AS borrow_volume_raw, 
   -- Total borrow volume (USD) = Each borrow amount × corresponding hourly price, then aggregated
   SUM(
      (COALESCE(mb.raw_amount, 0) / POWER(10, COALESCE(am.decimals, 0)))  -- Convert raw token amount to standardized unit using its decimals
      * COALESCE(hp.price, 0)                                             -- Use the corresponding hourly price; fallback to 0 if the price is missing
    ) AS borrow_volume_usd
  FROM marginfi_borrows mb -- Borrow event table (user perspective)
  INNER JOIN token_info ti
    ON mb.account_address = ti.account_address             -- Map the token ATA to its corresponding mint address
  INNER JOIN asset_metadata am
    ON ti.mint = am.token_address                          -- Retrieve the token's decimals information
  LEFT JOIN hp_final_prices hp
    ON am.token_address = hp.token_address
    AND hp.hour = mb.borrow_time   --  Precisely match the hourly price corresponding to the borrow timestamp
  GROUP BY ti.mint              -- Group and aggregate by token
)
--------------------------------------------------------------------------------
-- 6. Borrow Volume Summary
--------------------------------------------------------------------------------

SELECT
  'borrow_volume_usd' AS metric,
  SUM(borrow_volume_usd) AS value 
FROM borrow_volume;
