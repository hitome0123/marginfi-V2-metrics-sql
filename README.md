本项目为参与 Artemis Analytics Data Analytics Bounty 专项而构建，所有指标均依据 Bounty 官方任务要求开发。

基于 Flipside 提供的 Solana 链上数据，系统计算 MarginFi 协议六大核心链上指标。

指标设计遵循链上金融行业标准，参考 Aave 等主流协议的估值逻辑，涵盖协议存取款、借贷余额、手续费收入等维度，支持复用与二次开发。

   1. 指标计算说明（Explanation of Each Metric）

   本项目共构建了 6 个核心指标，目标是以用户视角与协议视角双重维度，衡量 MarginFi 协议资产流动性与收入结构。

   所有指标的计算逻辑均以 链上事件驱动为核心，其通用计算框架可抽象为以下 5 步：

   通用计算框架如下：

   1️⃣ 事件提取：基于 fact_decoded_instructions 表，选取关键 event_type（如 lendingAccountDeposit、lendingAccountBorrow）。

   2️⃣ 账户提取：从事件中解析目标账户字段（如 signerTokenAccount, bankLiquidityVault, destinationTokenAccount）。

   3️⃣ mint 映射：借助 fact_token_balances 将账户地址映射到对应的 Token mint。

   4️⃣ 标准化处理：使用 ez_asset_metadata 获取 decimals，将 raw_amount 统一为标准单位。

   5️⃣ 价格换算：联合 ez_prices_hourly 和 fact_prices_ohlc_hourly，获取小时级价格，换算为 USD。

   其中 ，Total Value Locked（TVL）和Borrow Volume使用了用户视角，应用的是用户视角的lendingAccountDeposit, lendingAccountWithdraw + signerTokenAccount。
   剩下的使用的是协议视角，bankLiquidityVault account

   特例说明：Protocol Fees & Liquidation Revenue 

   这两个指标不再从 amount + mint + price 简单推导 USD，而是采用 “事件 ➝ feeVault 地址 ➝ owner 确认 ➝ 实际到账 ➝ USD 价格” 的完整链路，如下：

   1️⃣ 事件提取：从 fact_decoded_instructions 中筛选 lendingPoolCollectBankFees；

   2️⃣ 账户定位：flatten decoded_instruction.accounts，找到 feeVault 或 insuranceVault；
   
   3️⃣ 归属确认：使用 fact_token_account_owners 获取这些 vault 地址的 owner；

   4️⃣ 转账匹配：联动 fact_transfers 表，找出 tx_id + owner 对应的真实到账记录；

   5️⃣ 金额换算：将到账金额结合转账发生时间，获取 USD 等值（优先主价格源，fallback 到 close）。

   2. 数据源说明（Data Sources Used）：
   
   为保障指标的准确性与可复现性，本项目使用 Flipside 提供的 Solana 多张核心链上表格，按用途分为如下几类：

   Instruction 类：

   solana.core.fact_decoded_instructions：所有核心链上事件的主表

   solana.core.fact_token_account_owners：确认 feeVault / insuranceVault 等账户的 token 持有人

   资产类：

   solana.core.fact_token_balances：账户地址与对应 mint 的映射

   solana.price.ez_asset_metadata：获取 symbol、decimals 等元数据

   价格类：

   solana.price.ez_prices_hourly：主价格源，提供小时级 token 价格

   solana.price.fact_prices_ohlc_hourly：辅助价格源，用于 fallback 逻辑

   转账类：

   solana.core.fact_transfers：所有 token 的实际 inflow / outflow 转账记录

   3. 关键假设说明（Key Assumptions / Fallback Strategy）

   为保障指标计算的稳定性与行业一致性，本项目在数据处理与估值过程中遵循以下默认规则与兜底策略：

   1️⃣ 统一事件来源： 所有 MarginFi 协议相关链上行为统一从 solana.core.fact_decoded_instructions 表中提取，该表覆盖 MarginFi V2 全部事件。

   2️⃣ 流量类与收入类指标使用事件价格估值： 包括 Borrow Volume、Deposit Volume、Protocol Fees、Revenue 等事件类指标，均采用事件发生时的价格计算 USD 金额，确保估值与链上操作时间一致。

   3️⃣ 余额类指标使用最新价格估值： 如 TVL、Borrow Outstanding、Deposit Outstanding 等余额快照类指标，使用最近一小时价格进行估值，反映当前系统状态。

   4️⃣ 主价格源优先级： 首选使用 solana.price.ez_prices_hourly，该表覆盖广、时效性强，精度为小时；若主表价格缺失，则 fallback 到 solana.price.fact_prices_ohlc_hourly 的 close 字段。

   5️⃣ decimals 缺失补全逻辑： 若 solana.price.ez_asset_metadata 中缺失某 token 的 decimals 字段，则通过 symbol 模糊匹配（如带 "usd" 默认 6 位），或链上查询核实进行兜底。

   6️⃣ 价格时间粒度统一： 所有价格处理统一进行 DATE_TRUNC('hour', timestamp)，确保估值对齐。

   7️⃣ 默认查询窗口： 所有查询默认限制数据在近 30 天范围内，如需扩展请在 SQL 中手动修改时间过滤条件。

   8️⃣ 空值处理策略： 所有价格、数量、精度等字段均通过 COALESCE() 包裹，确保查询健壮性与容错性。

   4. 查询运行方式（How to Run/Test the Queries）

   本项目中所有 SQL 查询均可在 Flipside Studio 上运行与测试，以下为具体使用方式说明：

   平台： 使用 Flipside Studio（https://flipsidecrypto.xyz）

   数据集限制： 默认限制为近 30 天内的链上事件数据，以提升查询效率。可通过修改 WHERE 子句手动放宽时间范围。

   查询入口： 在 Web UI 页面输入 SQL 并运行，所有查询结构已模块化，支持按需复制粘贴复用。

   输出字段： 查询结果统一输出为：metric_name（指标名称） + usd_value（对应 USD 金额），便于汇总展示与可视化分析。

所有查询脚本均采用标准 SQL 编写，字段命名统一，具备良好可维护性与扩展性。
