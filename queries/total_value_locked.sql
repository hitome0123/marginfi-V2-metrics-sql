


WITH deposit_actions AS (
  SELECT 
    a.event_type,
    a.block_timestamp,
    a.tx_id,
    -- 主要账户角色提取并命名（可用于 transfer 匹配）
    -- MAX(CASE WHEN acc.value:"name"::STRING = 'bankLiquidityVault' THEN acc.value:"pubkey"::STRING END) AS vault_address,
    -- MAX(CASE WHEN acc.value:"name"::STRING = 'signerTokenAccount' THEN acc.value:"pubkey"::STRING END) AS user_token_account,
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
    AND a.event_type = 'lendingAccountDeposit' 
  GROUP BY 
    a.event_type, a.block_timestamp, a.tx_id
),
withdraw_actions AS (
  SELECT 
    a.event_type,
    a.block_timestamp,
    a.tx_id,
    -- 主要账户角色提取并命名（可用于 transfer 匹配）
    -- MAX(CASE WHEN acc.value:"name"::STRING = 'bankLiquidityVault' THEN acc.value:"pubkey"::STRING END) AS vault_address,
    -- MAX(CASE WHEN acc.value:"name"::STRING = 'signerTokenAccount' THEN acc.value:"pubkey"::STRING END) AS user_token_account,
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") acc
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
    AND a.event_type = 'lendingAccountWithdraw' 
  GROUP BY 
    a.event_type, a.block_timestamp, a.tx_id
),
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
--------------------------------------------------------------------------------
deposit_transfer_actions AS (
  SELECT  distinct 
    vo.block_timestamp, -- Timestamp of the fee transfer
    vo.tx_id,         -- Transaction ID
    t.mint,           -- Token mint
    t.amount,
    'deposit' as type
  FROM deposit_actions vo 
  INNER JOIN (
  SELECT *
  FROM solana.core.fact_transfers
) t
    ON vo.tx_id = t.tx_id -- Match transfers going to vault owner in the same transaction
),
withdraw_transfer_actions AS (
  SELECT  distinct 
    wa.block_timestamp, -- Timestamp of the fee transfer
    wa.tx_id,         -- Transaction ID
    t.mint,           -- Token mint
    t.amount,
    'withdraw' as type
  FROM withdraw_actions wa
  INNER JOIN (
  SELECT *
  FROM solana.core.fact_transfers
) t
    ON wa.tx_id = t.tx_id -- Match transfers going to vault owner in the same transaction
)
select * from withdraw_transfer_actions 
union all 
select * from deposit_transfer_actions 
where tx_id = 'HgEwXbGTPYuxhiacV9hco8p4HgPTE7PmiNPFbaxm1nr4XiA7XgBeiJEKb5eZBJeT2nwtx2xYKAgt26fopWRo41G'

---改到这！看看是不是同一个tx_id,mint 和 amount 都是一样的，如果是的话，就不用在tx_id的记录中找哪一条是真正的！！！
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
TVL AS (
  SELECT 
    ti.mint,         -- Token mint address (unique identifier)
    am.symbol,       -- Token symbol
  
    -- Raw Total amount locked amount (not normalized by decimals), calculated as: deposit - withdraw    
    SUM(
      CASE 
        WHEN ma.event_type = 'lendingAccountDeposit' THEN COALESCE(ma.raw_amount, 0)
        WHEN ma.event_type = 'lendingAccountWithdraw' THEN -COALESCE(ma.raw_amount, 0)
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
  GROUP BY ti.mint, am.symbol
)

--------------------------------------------------------------------------------
-- 6. Total Value Locked  Summary
--------------------------------------------------------------------------------
SELECT
  'TVL' AS metric,
  SUM(net_amount_usd) AS value 
FROM TVL;




