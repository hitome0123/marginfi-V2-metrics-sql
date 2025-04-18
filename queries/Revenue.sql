--------------------------------------------------------------------------------
-- 1. Extract feeVault and insuranceVault addresses from protocol-level fee collection events
-- Source: solana.core.fact_decoded_instructions
-- 
-- Description:
--   - This query identifies the token accounts used by MarginFi to collect fees.
--   - Specifically filters instructions with event_type = 'lendingPoolCollectBankFees'.
--   - Uses LATERAL FLATTEN to expand account arrays and extract specific vault addresses:
--       • 'feeVault': where protocol fees are sent
--       • 'insuranceVault': where liquidation revenue is accumulated
--   - Applies MAX(CASE ...) as an aggregation trick to extract a single matching value
--     for each tx_id. This avoids duplication and ensures reliable joins downstream.
--------------------------------------------------------------------------------
WITH vault_addresses AS (
  SELECT 
    a.block_timestamp,  -- Timestamp of the fee collection instruction
    a.tx_id,            -- Transaction ID of the instruction (used for later joins)
    MAX(CASE 
        WHEN acc.value:"name"::STRING = 'feeVault'
        THEN acc.value:"pubkey"::STRING 
    END) AS fee_vault_address, 
    MAX(CASE 
        WHEN acc.value:"name"::STRING = 'insuranceVault' 
        THEN acc.value:"pubkey"::STRING 
  END) AS insurance_vault_address
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc -- Flatten account list from decoded 
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingPoolCollectBankFees' 
  GROUP BY a.block_timestamp, a.tx_id
),
--------------------------------------------------------------------------------
-- 2. Link feeVault and insuranceVault to their respective owners (actual token recipients)
-- Source: solana.core.fact_token_account_owners
--
-- Description:
--  - This step maps each extracted vault address (feeVault / insuranceVault) to its token account owner.
--  - Owners are the real recipients of token transfers and represent the entities receiving protocol or liquidation revenue.
--  - This information is critical for correctly attributing incoming token transfers to specific revenue types.
--  - Both fee and insurance vaults are included to support accurate classification of all types of revenue.
--------------------------------------------------------------------------------
vault_owners AS (
  SELECT 
    va.block_timestamp AS transfer_time,    -- Timestamp of the fee collection event (used for later price matching)
    va.tx_id,                               -- Transaction ID of the fee collection event
    va.fee_vault_address,                   -- SPL token account address for the fee vault
    va.insurance_vault_address,            -- SPL token account address for the insurance vault
    fao.owner AS fee_vault_owner,          -- Owner of the fee vault (true protocol fee recipient)
    iao.owner AS insurance_vault_owner     -- Owner of the insurance vault (recipient of liquidation revenue)
  FROM vault_addresses va

  -- Join fee vault token account with its corresponding owner (via the token registry table)
  LEFT JOIN solana.core.fact_token_account_owners fao
    ON va.fee_vault_address = fao.account_address

  -- Join insurance vault token account with its owner
  LEFT JOIN solana.core.fact_token_account_owners iao
    ON va.insurance_vault_address = iao.account_address

  -- Filter out vaults with no resolved owners (likely incomplete or non-SPL accounts)
  WHERE fao.owner IS NOT NULL OR iao.owner IS NOT NULL
),
--------------------------------------------------------------------------------
-- 3. Trace actual revenue-related token transfers triggered by protocol operations
-- Source: solana.core.fact_transfers
-- Description:
--   - Joins feeVault and insuranceVault owner addresses with actual token transfers.
--   - Classifies revenue type as 'Protocol Fees' or 'Liquidation Revenue' based on the recipient.
--   - Filters only positive-value transfers to ensure actual fund movement is captured.
--------------------------------------------------------------------------------

transfer_actions AS (
  SELECT  
    vo.transfer_time,       -- Timestamp of the original fee collection or liquidation event
    vo.tx_id,               -- Transaction ID used to correlate with transfer activity
    t.mint,                 -- Token mint address
    t.amount,               -- Transferred amount (already normalized to human-readable units)
    t.tx_to,                -- Destination address of the transfer

    -- Classify revenue type based on who received the transfer
    CASE 
      WHEN t.tx_to = vo.fee_vault_owner THEN 'Protocol Fees'         -- Protocol income
      WHEN t.tx_to = vo.insurance_vault_owner THEN 'Liquidation Revenue'  -- Insurance-related liquidation income
      ELSE 'Other'  -- Unknown or irrelevant recipient
    END AS revenue_type
  FROM vault_owners vo
  LEFT JOIN solana.core.fact_transfers t 
    ON vo.tx_id = t.tx_id     -- Join on transaction ID to match vault event and corresponding transfer
  WHERE t.amount > 0          -- Ensure only real, positive-value transfers are considered
)
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
-- 5. Match transfer records with historical prices to calculate revenue in USD
-- Sources:
--   - transfer_actions: identifies token transfers classified as Protocol Fees or Liquidation Revenue
--   - historical_prices: hourly token prices in USD
--
-- Logic:
--   - Join token transfers with the corresponding hourly price based on transfer time
--   - Multiply token amount by hourly price to convert to USD
--   - Filter only relevant revenue types: 'Protocol Fees' and 'Liquidation Revenue'
--------------------------------------------------------------------------------
Revenue_volume AS (
SELECT 
  ta.transfer_time,        -- Timestamp when the revenue was received (rounded to hour)
  ta.tx_id,                -- Corresponding transaction ID
  ta.revenue_type,         -- Type of revenue (Protocol Fees or Liquidation Revenue)
  ta.mint,                 -- Token mint address
  ta.amount,               -- Token amount received (already normalized to human-readable units)
  hp.price,                -- Hourly token price in USD
  (ta.amount * hp.price) AS revenue_amount_usd  -- Revenue in USD = token amount × hourly price
FROM transfer_actions ta
LEFT JOIN historical_prices hp
  ON ta.mint = hp.token_address           -- Join on token mint address
  AND hp.hour = DATE_TRUNC('hour', ta.transfer_time)  -- Match price by exact hour of transfer
WHERE ta.revenue_type IN ('Protocol Fees', 'Liquidation Revenue')  -- Only include relevant revenue types
ORDER BY ta.transfer_time DESC  -- Sort results by most recent revenue events
)   
--------------------------------------------------------------------------------
-- 6. Revenue Volume Summary
--------------------------------------------------------------------------------

SELECT
  'revenue_amount_usd' AS metric,
  SUM(revenue_amount_usd) AS value 
FROM Revenue_volume;


