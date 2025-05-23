--------------------------------------------------------------------------------
-- 1. Extract decoded deposit and withdrawal instructions from MarginFi
-- Source: solana.core.fact_decoded_instructions
--
-- Total Value Locked (TVL) measures the total value of assets currently held within the protocol.
-- In MarginFi, TVL is derived by aggregating two user-level interactions:
--   - 'lendingAccountDeposit': When users deposit funds into the protocol → TVL increases
--   - 'lendingAccountWithdraw': When users withdraw funds from the protocol → TVL decreases
--
-- This section extracts the raw token amount (before decimal normalization) and the user’s
-- SPL token account (signerTokenAccount) from both events. These values are used in the
-- subsequent normalization and price conversion steps to compute TVL in USD.
--------------------------------------------------------------------------------
WITH marginfi_actions AS (
  SELECT 
    a.event_type,  -- Event type: lendingAccountDeposit or lendingAccountWithdraw
    a.decoded_instruction:"args":"amount"::FLOAT AS raw_amount,  -- Raw amount (raw, not normalized by decimals)
    acc.value:"pubkey"::STRING AS account_address  -- The account address used by the user
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc -- Flatten account list from decoded instruction
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi v2 contract address
    AND a.event_type IN ('lendingAccountDeposit', 'lendingAccountWithdraw')  -- Filter deposit and withdrawal instructions
    AND acc.value:"name"::STRING = 'signerTokenAccount'  -- Signer's token ATA

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
  FROM marginfi_actions ma
  WHERE ma.account_address = tf.account_address
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
-- 4. Retrieve latest price for each token (used in Borrow Outstanding calculation)
-- Only the latest hour’s price is needed, not historical hourly prices
-- Priority:
--   - Primary: ez_prices_hourly
--   - Fallback: fact_prices_ohlc_hourly (close)
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
  SELECT 
    dm.token_address,  
    f.close AS price   -- Fallback price: use OHLC close as hourly consensus price
  FROM solana.price.fact_prices_ohlc_hourly f
  INNER JOIN solana.price.dim_asset_metadata dm 
    ON f.asset_id = dm.asset_id
  WHERE dm.blockchain = 'solana'
    AND f.hour = (  -- Same as above: fetch only the most recent hourly data
      SELECT MAX(hour) 
      FROM solana.price.fact_prices_ohlc_hourly
    )
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
-- 5. Compute Token-Level Net Deposits (TVL Core Calculation)
-- Source:
--   - marginfi_actions: decoded deposit and withdrawal events (user token account level)
--   - token_info: to map token account to mint
--   - asset_metadata: to get decimals and token symbol
--   - lp_final_prices: latest price per token (USD)
--
-- Description:
--   - This step calculates the Total Value Locked (TVL) per token by aggregating:
--       Raw deposits - withdrawals (from user accounts)
--   - The result is expressed in:
--       a) net_amount_raw (not normalized by decimals)
--       b) USD value (normalized using decimals × latest price)
--   - Deposits add to TVL, withdrawals subtract from it.
--   - COALESCE is used to ensure all missing values default safely to 0.
--------------------------------------------------------------------------------
TVL AS ( SELECT * FROM(
  SELECT 
    ti.mint,         -- Token mint address (unique identifier)
    am.symbol,       -- Token symbol
  
    -- Raw Total amount locked amount (not normalized by decimals), calculated as: deposit - withdraw    
    SUM(
      CASE 
        WHEN ma.event_type = 'lendingAccountDeposit' THEN COALESCE(ma.raw_amount, 0)/POWER(10, COALESCE(am.decimals, 0))
        WHEN ma.event_type = 'lendingAccountWithdraw' THEN -COALESCE(ma.raw_amount, 0)/POWER(10, COALESCE(am.decimals, 0))
        ELSE 0
      END
    ) AS net_amount_raw,

   -- Total Value Locked amount in USD = normalized token amount × latest price
    SUM(
      CASE 
        WHEN ma.event_type = 'lendingAccountDeposit' THEN 
        (COALESCE(ma.raw_amount, 0) 
        / POWER(10, COALESCE(am.decimals, 0)))  -- Normalize using token decimals
      * COALESCE(lp.price, 0)                   -- Use latest price; fallback to 0 if missing
        WHEN ma.event_type = 'lendingAccountWithdraw' THEN 
          (-COALESCE(ma.raw_amount, 0) 
         / POWER(10, COALESCE(am.decimals, 0))) -- Normalize using token decimals
        * COALESCE(lp.price, 0)                 -- Use latest price; fallback to 0 if missing
        ELSE 0
      END
    ) AS net_amount_usd
  FROM marginfi_actions ma
  LEFT JOIN token_info ti ON ma.account_address = ti.account_address -- Map the token ATA to its corresponding mint address
  LEFT JOIN asset_metadata am ON ti.mint = am.token_address   -- Retrieve the token's decimals information
  LEFT JOIN lp_final_prices lp ON am.token_address = lp.token_address -- Map token to latest price
  GROUP BY ti.mint, am.symbol)
WHERE 
  (
    --  Mainstream assets (USDC, SOL, mSOL, ETH, BTC)
    --     - Restrict net locked amount to less than 10 million (1e7) units
    --     - Prevent abnormal large balances from distorting TVL calculations
    (symbol IN ('USDC', 'SOL', 'mSOL', 'ETH', 'BTC') AND net_amount_raw < 1e7)
    OR
    --  Other minor assets (non-mainstream assets)
    --     - Allow net locked amount up to 0.1 billion (1e8) units
    --     - Accommodate the naturally large unit quantities of small-cap or meme tokens
    (symbol NOT IN ('USDC', 'SOL', 'mSOL', 'ETH', 'BTC') AND net_amount_raw < 1e8)
  )
  --  Global filtering: Net locked amount must be greater than 0.1 units
  --     - Remove "dust" balances (extremely small holdings) to maintain TVL accuracy
AND net_amount_raw > 0.1


)

--------------------------------------------------------------------------------
-- 6. Total Value Locked  Summary
-- This section summarizes the Total Value Locked (TVL) across all assets.
-- Final output values are converted and presented in millions (M USD) for better readability.
--------------------------------------------------------------------------------
SELECT
  'TVL_usd(M)' AS metric,
  SUM(net_amount_usd)/  1e6  AS value 
FROM TVL;
