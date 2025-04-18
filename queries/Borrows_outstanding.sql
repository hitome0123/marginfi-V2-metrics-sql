--------------------------------------------------------------------------------
-- 1. Extract Borrow operations related to BankLiquidityVault to calculate Borrow Outstanding (unpaid borrow balance)
-- Source: solana.core.fact_decoded_instructions
-- Note: This module takes a "protocol perspective". BankLiquidityVault is the core account of protocol asset flows.
-- By tracking this account, we can accurately trace borrow outflows at the protocol level for Outstanding calculation.
--------------------------------------------------------------------------------
WITH borrow_actions AS (
SELECT distinct
    a.event_type,  -- Event type: only lendingAccountBorrow, used to filter relevant events
    a.decoded_instruction:"args":"amount"::FLOAT AS borrow_raw_amount,  -- Borrow amount (not normalized)
    acc.value:"pubkey"::STRING AS borrow_token_address -- Internal protocol account for borrowing asset outflow (BankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc -- Flatten accounts to extract bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingAccountBorrow'  -- Borrow event
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  
    -- From protocol perspective, bankLiquidityVault represents the core account for asset inflow/outflow.
    -- It is used to measure borrow activity at the protocol level.
),
--------------------------------------------------------------------------------
-- 2. Extract Repay operations related to BankLiquidityVault for Borrow Outstanding calculation
-- Source: solana.core.fact_decoded_instructions
-- Note: From the protocol’s perspective, BankLiquidityVault is the receiving address for repayments.
-- This allows accurate tracking of total repayments made to the protocol.
--------------------------------------------------------------------------------
repay_actions AS (
  SELECT DISTINCT
    a.event_type, -- Event type: only lendingAccountRepay, used to filter user repayment events
    a.decoded_instruction:"args":"amount"::FLOAT AS repay_raw_amount,  -- Repay amount (not normalized)
    acc.value:"pubkey"::STRING AS repay_token_address  -- Protocol’s receiving address for repayments (BankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- Flatten account list to extract bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Protocol Program ID
    AND a.event_type = 'lendingAccountRepay'  -- Repay Event
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  
    -- From protocol perspective, bankLiquidityVault is the central account for asset flow.
    -- In repayments, this address reflects user funds being returned to the protocol.
),

--------------------------------------------------------------------------------
-- 3. Extract Liquidate operations related to BankLiquidityVault to calculate Borrow Outstanding
-- Source: solana.core.fact_decoded_instructions
-- Note: From a protocol perspective, bankLiquidityVault is the actual receiving account for seized assets.
-- During liquidation, this account receives a portion of the user’s collateral, reducing their outstanding debt.
--------------------------------------------------------------------------------
liquidate_actions AS (
  SELECT DISTINCT
    a.event_type,  -- Event type: lendingAccountLiquidate
    a.decoded_instruction:"args":"assetAmount"::FLOAT AS liquidate_raw_amount,  -- Liquidated asset amount (not normalized)
    acc.value:"pubkey"::STRING AS liquidate_token_address  -- Custody address for liquidated asset (i.e., BankLiquidityVault)
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- Flatten account list to extract bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  
    AND a.event_type = 'lendingAccountLiquidate'  -- liquidate Event
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  
    -- From protocol perspective, bankLiquidityVault is the central account for asset flow.
    -- In liquidate, this address reflects user funds being liquidated to the protocol.
),
--------------------------------------------------------------------------------
-- 4. Map Token Account to Mint for identifying token types
-- Source: solana.core.fact_token_balances
-- This table maps each token account (SPL ATA) to its corresponding mint address
-- Note: Token accounts (ATA) are linked to their mint addresses for metadata lookup
--------------------------------------------------------------------------------
token_info AS (
  SELECT distinct
    account_address,  -- Token account (SPL ATA)
    mint              -- Associated token mint (unique identifier)
  FROM solana.core.fact_token_balances 
),

--------------------------------------------------------------------------------
-- 5. Build asset metadata: get symbol and decimals for each token
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
-- 6. Retrieve latest price for each token (used in Borrow Outstanding calculation)
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
      WHERE blockchain = 'solana'
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
-- 7. Compute Borrow Outstanding (remaining borrow balance in USD)
-- Source: protocol-perspective events from BankLiquidityVault
-- Logic: Outstanding = borrow - repay - liquidate, then multiply by latest token price
--------------------------------------------------------------------------------
outstanding_volume AS (
  SELECT
    ti.mint,                         -- Token  mint address
    -- Raw outstanding amount (not normalized by decimals), calculated as: borrow - repay - liquidate    
  SUM(
      COALESCE(ba.borrow_raw_amount, 0) 
      - COALESCE(ra.repay_raw_amount, 0) 
      - COALESCE(la.liquidate_raw_amount, 0)
    ) AS outstanding_volume_raw,
  -- Outstanding balance (in USD) = normalized token amount × latest price (from lp_final_prices)
  SUM(
      (
        COALESCE(ba.borrow_raw_amount, 0)
        - COALESCE(ra.repay_raw_amount, 0)
        - COALESCE(la.liquidate_raw_amount, 0)
      ) / POWER(10, COALESCE(am.decimals, 0))        -- Normalized token amount (based on decimals)
        * COALESCE(lf.price, 0)                      -- Use the latest price; if unavailable, default to 0
    ) AS outstanding_volume_usd
  FROM borrow_actions ba
  LEFT JOIN token_info ti 
    ON ba.borrow_token_address = ti.account_address    -- Map the token ATA to its corresponding mint address
  LEFT JOIN asset_metadata am 
    ON ti.mint = am.token_address                      -- Retrieve the token's decimals information
  LEFT JOIN lp_final_prices lf 
    ON am.token_address = lf.token_address             
  -- Repay and liquidation events are also matched based on the same token address (mint)
  LEFT JOIN repay_actions ra 
    ON am.token_address = ra.repay_token_address
  LEFT JOIN liquidate_actions la 
    ON am.token_address = la.liquidate_token_address
  GROUP BY ti.mint             
)
--------------------------------------------------------------------------------
-- 6. Borrow Outstanding Summary
--------------------------------------------------------------------------------

SELECT
  'outstanding_volume_usd' AS metric,
  SUM(outstanding_volume_usd) AS value
FROM outstanding_volume;

