--------------------------------------------------------------------------------
-- 1. Extract feeVault addresses from the protocol fee collection events
-- Source: solana.core.fact_decoded_instructions
-- This part identifies the "feeVault" account involved in the
-- 'lendingPoolCollectBankFees' events as defined in MarginFi protocol
-- 'lendingPoolCollectBankFees' indicates the transaction where protocol fees are collected from the bank's liquidity vault
--------------------------------------------------------------------------------
WITH vault_addresses AS (
  SELECT 
    a.block_timestamp,  -- Timestamp of the fee collection event
    a.tx_id,            -- Transaction ID of the instruction
    -- Extract feeVault address (only one per instruction)
   MAX(CASE 
      WHEN acc.value:"name"::STRING = 'feeVault' 
      THEN acc.value:"pubkey"::STRING 
    END) AS fee_vault_address  --  Use MAX(CASE...) to pick the correct pubkey after flattening
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 Program ID
    AND a.event_type = 'lendingPoolCollectBankFees'
  GROUP BY a.block_timestamp, a.tx_id
),
--------------------------------------------------------------------------------
-- 2. Link feeVault to its owner using the token account registry
-- Source: solana.core.fact_token_account_owners
-- This determines the real receiver of the fee tokens by resolving who owns
-- the feeVault SPL token account (owner = protocol fee receiver)
--------------------------------------------------------------------------------
vault_owners AS (
  SELECT 
    va.block_timestamp AS transfer_time, -- Timestamp of fee collection
    va.tx_id,      --Transaction ID used for joining with related token transfer records later in the query
    va.fee_vault_address,   -- Vault address that holds the tokens
    fao.owner AS fee_vault_owner --The owner of that vault (true recipient of the tokens)
  FROM vault_addresses va
  LEFT JOIN solana.core.fact_token_account_owners fao
  -- Match fee vault token account with its corresponding owner (SPL token ownership resolution)
    ON va.fee_vault_address = fao.account_address 
  -- Filter out vaults that do not have an associated owner (non-standard or incomplete token accounts)
  WHERE fao.owner IS NOT NULL
),
--------------------------------------------------------------------------------
-- 3. Trace protocol fee inflows to the vault owner
-- Source: solana.core.fact_transfers
-- Description:
--   - Joins fee collection transactions with actual token transfers.
--   - Ensures the destination (tx_to) of the transfer is the feeVault owner.
--   - Filters only transactions where:
--       a) The tx_id matches the original fee collection event.
--       b) The vault owner is the recipient of the transfer.
--       c) The amount is greater than 0 (ensures only real transfers are considered).
-- This ensures that only genuine protocol-level inflows triggered by fee collection logic are captured.
-- Explanation: Historical prices are used for Protocol Fees to reflect the actual value at the time of the transaction.
-- This ensures that the calculation of fees and revenue is consistent with the value agreed upon during the transaction,
-- rather than being influenced by later market fluctuations.
--------------------------------------------------------------------------------
transfer_actions AS (
  SELECT  
    vo.transfer_time, -- Timestamp of the fee transfer
    vo.tx_id,         -- Transaction ID
    t.mint,           -- Token mint
    t.amount,         -- Verified: amount field is already normalized to human-readable token units
    t.tx_to           -- Transfer destination address
  FROM vault_owners vo 
  LEFT JOIN solana.core.fact_transfers t 
    ON vo.tx_id = t.tx_id AND vo.fee_vault_owner = t.tx_to -- Match transfers going to vault owner in the same transaction
  WHERE t.amount > 0  -- ensures only real transfers are considered
),

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
-- 4. Calculate Protocol Fees in USD
-- Join token transfers with matched prices to compute fee value in USD
-- Assumes token amounts are already human-readable (optional: normalize using decimals)
--------------------------------------------------------------------------------

final_protocol_fees AS (
  SELECT 
    ta.transfer_time, -- Timestamp of the fee transfer
    ta.tx_id,         -- Transaction ID
    ta.mint,          -- Token mint
    ta.amount ,       -- Verified: amount field is already normalized to human-readable token units
    hp.price,         -- Hourly token price (USD)
    (ta.amount  * hp.price ) AS protocol_fees_usd, -- Total protocol fee in USD
  FROM transfer_actions ta 
  LEFT JOIN hp_final_prices hp
    ON ta.mint = hp.token_address 
    AND hp.hour = DATE_TRUNC('hour', ta.transfer_time) -- Match token price based on the exact hour of the transfer
)
--------------------------------------------------------------------------------
-- 5. Protocol_fees_usd Summary
-- This section summarizes the Protocol_fees across all assets.
-- Final output values are converted and presented in millions (M USD) for better readability.
--------------------------------------------------------------------------------

SELECT
  'protocol_fees_usd(M)' AS metric,
  SUM(protocol_fees_usd)/1e6 AS value 
FROM final_protocol_fees;
