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
--------------------------------------------------------------------------------
-- 6. 构建资产元信息（asset_metadata）：获取每个 token 的 symbol 和 decimals 精度
-- 逻辑顺序：
--   (1) 优先使用 ez_asset_metadata 中官方 decimals；
--   (2) 若缺失，则根据 symbol 模糊匹配推断；
--   (3) 若仍无法判断，则对查证过的 token_address 进行手动 fallback；
-- 特别说明：
--   - 多数 LP Token 与其原生 Token 共用 decimal，（如 USDC-LP、ETH-LP 等）
--   - pump 相关 meme token 经验统一为 6
--   - fallback 中涉及的 token 已手动查证确认
--------------------------------------------------------------------------------
asset_metadata AS (
  SELECT 
    ezm.token_address, -- Token 的地址
    ezm.symbol,        -- Token 的 symbol（用于推断）
    COALESCE(
      ezm.decimals,  -- Prioritize the official decimals
      -- If missing, try symbol-based inference
      CASE 
        WHEN LOWER(ezm.symbol) LIKE '%usd%' THEN 6  -- USDC / USDT / USD LP/Wrapped系列资产，与原生稳定币统一为 6（如 LP-USDC、cUSDC）
        WHEN LOWER(ezm.symbol) LIKE '%eth%' THEN 8  -- ETH LP/Wrapped系列资产，与原生ETH统一为 8
        WHEN LOWER(ezm.symbol) LIKE '%sol' THEN 9   -- ETH LP/Wrapped系列资产，与原生SOL统一为 9
        -- PumpFun 系列 meme token，一般为 6（通过 symbol 或地址中包含 "pump" 判定）
        WHEN LOWER(ezm.symbol) LIKE '%pump%' THEN 6 
        WHEN LOWER(ezm.token_address) LIKE '%pump' THEN 6
        -- Fallback：部分常见 Token 的硬编码处理（经人工查证，确保 decimals 正确)
        WHEN ezm.token_address IN( 'ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY' 
                                   ,'CTJf74cTo3cw8acFP1YXF3QpsQUUBGBjh2k2e8xsZ6UL'
                                   ,'3S8qX1MsMqRbiwKg2cQyx7nis1oHMgaCuc9c4VfvVdPN') THEN 6
        WHEN ezm.token_address IN( 'oreoN2tQbHXVaZsr3pf66A48miqcBXCDJozganhEJgz' 
                                   ,'HRw8mqK8N3ASKFKJGMJpy4FodwR3GKvCFKPDQNqUNuEP'
                                   ,'CLoUDKc4Ane7HeQcPpE3YHnznRxhMimJ4MyaUqyHFzAu'
                                   ,'8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5') THEN 9
        ELSE NULL
      END
    ) AS decimals -- 最终 decimals 精度值
  FROM solana.price.ez_asset_metadata ezm
),


--------------------------------------------------------------------------------
-- 7. 汇总 Token 的历史价格（用于计算 Borrow Volume）
-- 价格来源：
--   主价格源：solana.price.ez_prices_hourly（优先使用）
--   备价格源：solana.price.fact_prices_ohlc_hourly（OHLC 收盘价）
-- 合并策略：按 token_address + hour 对齐，优先使用主价格
-- 特别说明：
-- 该模块构建每小时级别的 Token 定价体系，为借款事件按小时匹配价格，确保 USD 估值精度。
--------------------------------------------------------------------------------
-- ① 主价格源（hourly 粒度）
-- 来源：链上主流价格表 ez_prices_hourly
-- 优点：覆盖率高、稳定性强
hp_main_prices AS (
  SELECT 
    token_address,             -- Token 的合约地址
    price,                     -- 小时级别的价格（USD）
    1 AS source_priority,      -- 设置为主价格源，优先级为 1
    hour                       -- 对应小时，用于与 borrow 时间对齐
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana'  -- 限定只取 Solana 链上的价格数据
),

-- ② 备价格源（OHLC 收盘价）
-- 来源：fact_prices_ohlc_hourly 表中 close 字段
-- 用于主价格缺失时 fallback 替代
hp_backup_prices AS (
  SELECT 
    dm.token_address,          -- Token 地址
    f.close AS price,          -- 收盘价 close 作为补充价格
    2 AS source_priority,      -- 备份价格源，优先级为 2
    f.hour                     -- 小时维度，与主源对齐
  FROM solana.price.fact_prices_ohlc_hourly f
  JOIN solana.price.dim_asset_metadata dm 
    ON f.asset_id = dm.asset_id
  WHERE dm.blockchain = 'solana'  -- 限定 Solana 链
),
-- ③ 合并主备价格
-- 按 token_address + hour 维度 full outer join
-- 优先选取主价格，主价格为空则使用备份价格
hp_final_prices AS (
  SELECT
    COALESCE(mp.token_address, bp.token_address) AS token_address,  -- 合并 token 地址，优先主价格源
    COALESCE(mp.hour, bp.hour) AS hour,                             -- 合并小时字段
    COALESCE(mp.price, bp.price) AS price                           -- 优先使用主价格，否则 fallback 到备份价格
  FROM hp_main_prices mp
  FULL OUTER JOIN hp_backup_prices bp
    ON mp.token_address = bp.token_address     -- 按 token_address 对齐
   AND mp.hour = bp.hour                       -- 按小时字段对齐
),
  
--------------------------------------------------------------------------------
-- 8. 获取最新价格（用于计算 Borrow Outstanding）
-- 场景需求：仅需要每个 Token 当前最新价格（而非历史每小时价格）
-- 合并来源：
--   主价格源：ez_prices_hourly（优先）
--   备价格源：fact_prices_ohlc_hourly（收盘价）
-- 合并策略：按 token_address 对齐，优先使用主价格
--------------------------------------------------------------------------------
-- ① 主价格源（取最新一小时）
lp_main_prices AS (
  SELECT 
    token_address,     -- Token 地址
    price              -- 最新价格（USD）
  FROM solana.price.ez_prices_hourly
  WHERE blockchain = 'solana'
    AND hour = (  -- 仅取最新一小时
      SELECT MAX(hour) 
      FROM solana.price.ez_prices_hourly 
      WHERE blockchain = 'solana'
    )
),

-- ② 备价格源（取最新一小时的收盘价）
lp_backup_prices AS (
  SELECT 
    dm.token_address,  -- Token 地址（来自维度表）
    f.close AS price   -- 收盘价 close 字段
  FROM solana.price.fact_prices_ohlc_hourly f
  INNER JOIN solana.price.dim_asset_metadata dm 
    ON f.asset_id = dm.asset_id
  WHERE dm.blockchain = 'solana'
    AND f.hour = (  -- 同样只取最新小时
      SELECT MAX(hour) 
      FROM solana.price.fact_prices_ohlc_hourly
    )
),

-- ③ 合并主备价格源
lp_final_prices AS (
  SELECT 
    COALESCE(lm.token_address, lb.token_address) AS token_address,  -- 优先使用主价格中的 token 地址
    COALESCE(lm.price, lb.price) AS price                           -- 优先使用主价格；若缺失则使用备份价格
  FROM lp_main_prices lm  
  LEFT JOIN lp_backup_prices lb
    ON lm.token_address = lb.token_address
),
--------------------------------------------------------------------------------
-- 7. 计算 Borrow Volume（借款总量，按美元计）
-- 数据来源：用户视角的借款事件（destinationTokenAccount）
-- 核心逻辑：每笔借款金额（按 token decimals 标准化）× 当时小时级别价格，再求和
--------------------------------------------------------------------------------
borrow_volume AS (
  SELECT
    ti.mint,                     -- Token mint 地址（唯一标识）
    am.symbol,                  -- Token 符号，如 USDC、SOL
    SUM(COALESCE(mb.raw_amount, 0)) AS borrow_volume_raw,  -- 借款总量（未标准化）
    -- 借款总量（USD）= 每笔借款金额 × 对应小时价格，最终汇总
    SUM(
      (COALESCE(mb.raw_amount, 0) / POWER(10, COALESCE(am.decimals, 0)))  -- 将原始金额按 decimals 转换为标准 token 单位
      * COALESCE(hp.price, 0)                                             -- 使用对应小时价格，若缺失则按 0 保守处理
    ) AS borrow_volume_usd
  FROM marginfi_borrows mb
  LEFT JOIN token_info ti
    ON mb.account_address = ti.account_address             -- 将 token ATA 映射到对应的 mint
  LEFT JOIN asset_metadata am
    ON ti.mint = am.token_address                          -- 获取 token 的 symbol 和 decimals 信息
  LEFT JOIN hp_final_prices hp
    ON am.token_address = hp.token_address
    AND hp.hour = DATE_TRUNC('hour', mb.BLOCK_TIMESTAMP)   -- 精确匹配“借款时间”的小时级别价格
  GROUP BY ti.mint, am.symbol, am.decimals                 -- 按 token 分组汇总
),
--------------------------------------------------------------------------------
-- 8. 计算 Borrow Outstanding（未偿还借款总额，按 USD）
-- 数据来源：BankLiquidityVault（协议资产账户）视角，综合 Outstanding =  borrow - repay - liquidate
-- 核心逻辑：借出 - 偿还 - 清算 = 当前未还余额，再乘以最新价格换算为 USD
--------------------------------------------------------------------------------
outstanding_volume AS (
  SELECT
    ti.mint,                         -- Token 的 mint 地址（唯一标识符）
    am.symbol,                      -- Token 符号（如 USDC、SOL）
    -- 原始未偿还量（未除 decimals），= borrow - repay - liquidate
    SUM(
      COALESCE(ba.borrow_raw_amount, 0) 
      - COALESCE(ra.repay_raw_amount, 0) 
      - COALESCE(la.liquidate_raw_amount, 0)
    ) AS outstanding_volume_raw,
    -- 未偿还余额（USD）= 标准化 token 数量 × 最新价格（lp_final_prices）
    SUM(
      (
        COALESCE(ba.borrow_raw_amount, 0)
        - COALESCE(ra.repay_raw_amount, 0)
        - COALESCE(la.liquidate_raw_amount, 0)
      ) / POWER(10, COALESCE(am.decimals, 0))       -- 标准化 token 数量（根据 decimals）
        * COALESCE(lf.price, 0)                      -- 使用最新价格，若无价格则按 0 处理
    ) AS outstanding_volume_usd
  FROM borrow_actions ba
  LEFT JOIN token_info ti 
    ON ba.borrow_token_address = ti.account_address    -- 将协议内部账户映射到 mint
  LEFT JOIN asset_metadata am 
    ON ti.mint = am.token_address                      -- 获取 token 的 symbol 和 decimals
  LEFT JOIN lp_final_prices lf 
    ON am.token_address = lf.token_address             -- 最新价格映射表（lp: latest price）
  -- 偿还和清算事件，也基于相同的 token address（mint）
  LEFT JOIN repay_actions ra 
    ON am.token_address = ra.repay_token_address
  LEFT JOIN liquidate_actions la 
    ON am.token_address = la.liquidate_token_address
  GROUP BY ti.mint, am.symbol, am.decimals             -- 按 token 聚合结果
)

-- Borrow Volume 汇总
SELECT
  SUM(borrow_volume_usd) AS borrow_volume_usd
FROM borrow_volume;

-- Outstanding Volume 汇总
SELECT
  SUM(outstanding_volume_usd) AS outstanding_volume_usd
FROM outstanding_volume;

