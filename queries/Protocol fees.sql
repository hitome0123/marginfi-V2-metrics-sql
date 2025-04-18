--------------------------------------------------------------------------------
-- 1. Extract feeVault addresses from the protocol fee collection events
-- Source: solana.core.fact_decoded_instructions
-- This part identifies the "feeVault" account involved in the
-- 'lendingPoolCollectBankFees' events as defined in MarginFi protocol
--------------------------------------------------------------------------------
WITH vault_addresses AS (
  SELECT 
    a.block_timestamp,  -- Timestamp of the fee collection event
    a.tx_id,
    MAX(CASE WHEN acc.value:"name"::STRING = 'feeVault' THEN acc.value:"pubkey"::STRING END) AS fee_vault_address
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
    AND a.event_type = 'lendingPoolCollectBankFees'
  GROUP BY a.block_timestamp, a.tx_id
),

vault_owners AS (
  SELECT 
    va.block_timestamp AS transfer_time,
    va.tx_id,
    va.fee_vault_address,
    fao.owner AS fee_vault_owner
  FROM vault_addresses va
  LEFT JOIN solana.core.fact_token_account_owners fao
    ON va.fee_vault_address = fao.account_address
  WHERE fao.owner IS NOT NULL
),

transfer_actions AS (
  SELECT  
    vo.transfer_time,
    vo.tx_id,
    t.mint,
    t.amount,
    t.tx_to
  FROM vault_owners vo
  LEFT JOIN solana.core.fact_transfers t 
    ON vo.tx_id = t.tx_id AND vo.fee_vault_owner = t.tx_to
  WHERE t.amount > 0
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


final_protocol_fees AS (
  SELECT 
    ta.transfer_time,
    ta.tx_id,
    ta.mint,
    ta.amount ,
    hp.price,
    (ta.amount  * hp.price ) AS amount_usd,
    'Protocol Fees' AS revenue_type
  FROM transfer_actions ta
  LEFT JOIN hp_final_prices hp
    ON ta.mint = hp.token_address
    AND hp.hour = DATE_TRUNC('hour', ta.transfer_time)
)

SELECT * FROM final_protocol_fees
ORDER BY transfer_time DESC;
