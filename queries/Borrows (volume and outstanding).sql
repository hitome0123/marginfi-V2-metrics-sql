--------------------------------------------------------------------------------
-- 1. 提取 MarginFi 的 Borrow 事件，用于计算 Borrows_volume（借款总量）
-- 数据来源：solana.core.fact_decoded_instructions
-- 注：此部分从“用户视角”出发，反映 MarginFi 协议的实际借出资产流量
--------------------------------------------------------------------------------
WITH marginfi_borrows AS (
  SELECT 
    a.BLOCK_TIMESTAMP,  -- 借款时间，用于后续按小时匹配价格
    a.decoded_instruction:"args":"amount"::FLOAT AS raw_amount,  -- 借款金额（未除以 decimals）
    acc.value:"pubkey"::STRING AS account_address  -- 借款 Token ATA（destinationTokenAccount）
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- 展平指令中的账户列表
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- 指定 MarginFi V2 的 Program ID
    AND a.event_type = 'lendingAccountBorrow'  -- 借款事件类型
    AND acc.value:"name"::STRING = 'destinationTokenAccount'  -- 目标账户名为 destinationTokenAccount
    -- 从用户角度出发，每一笔 lendingAccountBorrow 中，destinationTokenAccount 表示实际的借出账户，
    -- 能清楚反映借款“流出协议”的真实流量。
),

--------------------------------------------------------------------------------
-- 2. 提取与 BankLiquidityVault 相关的Borrow操作事件，用于计算 Borrow Outstanding（未偿还借款余额）
-- 数据来源：solana.core.fact_decoded_instructions
-- 注：该模块从“协议视角”出发，BankLiquidityVault 是协议资金流动的核心账户，
--     借助该地址可精准追踪协议层面的借款流出行为，为 Outstanding 计算提供基础数据支持
--------------------------------------------------------------------------------
borrow_actions AS (
SELECT distinct
    a.event_type,  -- 事件类型：仅限 lendingAccountBorrow，用于后续筛选事件计算
    a.decoded_instruction:"args":"amount"::FLOAT AS borrow_raw_amount,  -- 借款金额（未归一化）
    acc.value:"pubkey"::STRING AS borrow_token_address  -- 协议内部用于接收借款资产的账户地址（BankLiquidityVault）
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- 展平账户列表，提取 bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 协议 Program ID
    AND a.event_type = 'lendingAccountBorrow'  -- 借款事件
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  
    -- 从系统视角出发，bankLiquidityVault 表示协议资产流入流出的核心账户，
    -- 可用于衡量“协议层面”上的借出行为。
),
--------------------------------------------------------------------------------
-- 3. 提取与 BankLiquidityVault 相关的Repay操作事件，用于计算 Borrow Outstanding
-- 数据来源：solana.core.fact_decoded_instructions
-- 注：从“协议视角”出发，BankLiquidityVault 作为借款偿还的接收地址，
--     可用于准确反映协议收到的还款总额
--------------------------------------------------------------------------------
repay_actions AS (
  SELECT DISTINCT
    a.event_type,  -- 事件类型：仅限 lendingAccountRepay，用于筛选用户还款行为
    a.decoded_instruction:"args":"amount"::FLOAT AS repay_raw_amount,  -- 偿还金额（未归一化）
    acc.value:"pubkey"::STRING AS repay_token_address  -- 协议接收偿还资产的账户地址（BankLiquidityVault）
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- 展平账户列表，提取 bankLiquidityVault
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi V2 协议 Program ID
    AND a.event_type = 'lendingAccountRepay'  -- 偿还事件
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  
    -- 从系统视角出发，bankLiquidityVault 是协议资产流动的核心账户，
    -- 在还款中表示用户将资产偿还给协议。
),


--------------------------------------------------------------------------------
-- 4. 提取与 BankLiquidityVault 相关的liquidate操作事件，用于计算 Borrow Outstanding（未偿还借款余额）
-- 数据来源：solana.core.fact_decoded_instructions
-- 注：该部分从“协议视角”出发，BankLiquidityVault 表示协议资产的实际接收账户，
--     清算操作中该账户收到被清算用户的部分资产，可用于衡量协议因强制收回而减少的未还借款
--------------------------------------------------------------------------------
liquidate_actions AS (
  SELECT DISTINCT
    a.event_type,  -- 事件类型（lendingAccountLiquidate）
    a.decoded_instruction:"args":"assetAmount"::FLOAT AS liquidate_raw_amount,  -- 被清算的资产数量（未除以 decimals）
    acc.value:"pubkey"::STRING AS liquidate_token_address  -- 被清算资产的托管地址（即 BankLiquidityVault）
  FROM solana.core.fact_decoded_instructions a,
       LATERAL FLATTEN(input => a.decoded_instruction:"accounts") AS acc  -- 展平账户字段
  WHERE a.program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'  -- MarginFi 协议 Program ID
    AND a.event_type = 'lendingAccountLiquidate'  -- 清算操作类型
    AND acc.value:"name"::STRING = 'bankLiquidityVault'  -- 目标账户必须是 BankLiquidityVault
),

--------------------------------------------------------------------------------
-- 5. 映射 Token Account 与 Mint 信息，用于后续识别 Token 类型
-- 数据来源：solana.core.fact_token_balances
-- 注：用户地址（account_address）本质上是 SPL Token 的 ATA（Associated Token Account），
--     与 mint 建立映射关系，用于后续关联 token metadata（如 symbol / decimals）
--------------------------------------------------------------------------------
token_info AS (
  SELECT DISTINCT
    account_address,  -- 用户的 token account（SPL ATA）
    mint              -- 该账户对应的 token mint（即 token 的唯一标识）
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
        WHEN LOWER(ezm.symbol) LIKE '%eth%' THEN 8
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


--------------------------------------------------------------------------------
-- 6️⃣ 主  备价格源合并 使用历史价格数据 用于计算Borrows_volume
--------------------------------------------------------------------------------
hp_main_prices AS (
  SELECT token_address, price, 1 AS source_priority, hour
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana'
),
hp_backup_prices AS (
  SELECT dm.token_address, f.close AS price, 2 AS source_priority, hour
  FROM solana.price.fact_prices_ohlc_hourly f
  JOIN solana.price.dim_asset_metadata dm ON f.asset_id = dm.asset_id
  WHERE dm.blockchain = 'solana'
),
hp_final_prices AS (
  SELECT
    COALESCE(mp.token_address, bp.token_address) AS token_address,
    COALESCE(mp.hour, bp.hour) AS hour,
    COALESCE(mp.price, bp.price) AS price
  FROM hp_main_prices mp
  FULL OUTER JOIN hp_backup_prices bp
    ON mp.token_address = bp.token_address
   AND mp.hour = bp.hour
),
-- 4. 使用历史价格数据 用于计算Outstanding_volume Retrieve the main price source for the token (latest hour) → ez_prices_hourly
lp_main_prices AS (
  SELECT token_address, price
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana'
    AND hour = (SELECT MAX(hour) FROM solana.price.ez_prices_hourly WHERE blockchain = 'solana')
),

-- 5. Retrieve backup price source → fact_prices_ohlc_hourly (original price close field)
lp_backup_prices AS (
  SELECT dm.token_address, f.close AS price
  FROM solana.price.fact_prices_ohlc_hourly f
  INNER JOIN solana.price.dim_asset_metadata dm
    ON f.asset_id = dm.asset_id
  WHERE f.hour = (SELECT MAX(hour) FROM solana.price.fact_prices_ohlc_hourly)
    AND dm.blockchain = 'solana'
),

-- 6. Merge main and backup price sources (prefer using main price)
lp_final_prices AS (
  SELECT 
    lm.token_address, 
    COALESCE(lm.price, lb.price) AS price  -- Prefer using main price (main_prices)
  FROM lp_main_prices lm  
  LEFT JOIN lp_backup_prices lb
    ON lm.token_address = lb.token_address  -- If main price is NULL, use backup price
),


--------------------------------------------------------------------------------
-- 7️⃣ 核心计算 Borrow Volume（原始 + USD）
--------------------------------------------------------------------------------
borrow_volume AS (
  SELECT
    ti.mint,
    am.symbol,
    SUM(mb.raw_amount) AS borrow_volume_raw,
    SUM(
      (mb.raw_amount / POWER(10, am.decimals)) * hp.price
    ) AS borrow_volume_usd
  FROM marginfi_borrows mb
  LEFT JOIN token_info ti
    ON mb.account_address = ti.account_address
  LEFT JOIN asset_metadata am
    ON ti.mint = am.token_address
  LEFT JOIN hp_final_prices hp
    ON am.token_address = hp.token_address
    AND hp.hour = DATE_TRUNC('hour', mb.BLOCK_TIMESTAMP)
  GROUP BY ti.mint, am.symbol, am.decimals
),
---8️⃣ 计算 Borrow Oustanding（USD）
outstanding_volume AS (
  SELECT
    ti.mint,
    am.symbol,
    SUM(coalesce(ba.borrow_raw_amount,0) -  COALESCE(ra.repay_raw_amount,0) - COALESCE(la.liquidate_raw_amount,0))  AS outstanding_volume_raw,
    SUM((coalesce(ba.borrow_raw_amount,0) -  COALESCE(ra.repay_raw_amount,0) - COALESCE(la.liquidate_raw_amount,0)) / POWER(10, am.decimals) * lf.price) AS outstanding_volume_usd
  FROM borrow_actions ba
  LEFT JOIN token_info ti ON ba.borrow_token_address = ti.account_address
  LEFT JOIN asset_metadata am ON ti.mint = am.token_address
  LEFT JOIN lp_final_prices lf ON am.token_address = lf.token_address
  LEFT JOIN repay_actions ra ON am.token_address = ra.repay_token_address
  LEFT JOIN liquidate_actions la ON am.token_address = la.liquidate_token_address
  GROUP BY ti.mint, am.symbol, am.decimals

)
--------------------------------------------------------------------------------
-- ✅ 输出
--------------------------------------------------------------------------------
SELECT
  sum(a.borrow_volume_usd) as borrow_volume_usd,
  sum(b.outstanding_volume_usd) as outstanding_volume_usd
FROM borrow_volume a 
FULL OUTER JOIN outstanding_volume b
ON a.mint = b.mint AND a.symbol = b.symbol ;
