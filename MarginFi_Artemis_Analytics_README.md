# 📊 MarginFi Protocol Metrics for Artemis Analytics Bounty

本项目为参与 [Artemis Analytics Data Analytics Bounty](https://artemis.xyz) 专项而构建，所有指标均依据 Bounty 官方任务要求开发。

基于 Flipside 提供的 Solana 链上数据，系统计算 MarginFi 协议六大核心链上指标。

指标设计遵循链上金融行业标准，参考 Aave 等主流协议的估值逻辑，涵盖协议存取款、借贷余额、手续费收入等维度，支持复用与二次开发。

---

## 📑 目录（Table of Contents）

- [1. 指标计算说明（Explanation of Each Metric）](#1-指标计算说明explanation-of-each-metric)
- [2. 数据源说明（Data Sources Used）](#2-数据源说明data-sources-used)
- [3. 关键假设说明（Key Assumptions / Fallback Strategy）](#3-关键假设说明key-assumptions--fallback-strategy)
- [4. 查询运行方式（How to Run/Test the Queries）](#4-查询运行方式how-to-runtest-the-queries)

---

## 1. 指标计算说明（Explanation of Each Metric）

本项目共构建了 6 个核心指标，目标是以用户视角与协议视角双重维度，衡量 MarginFi 协议资产流动性与收入结构。

所有指标的计算逻辑均以链上事件驱动为核心，其通用计算框架可抽象为以下 5 步：

### 通用计算框架如下：

1️⃣ **事件提取**：基于 `fact_decoded_instructions` 表，选取关键 `event_type`（如 `lendingAccountDeposit`、`lendingAccountBorrow`）。  
2️⃣ **账户提取**：从事件中解析目标账户字段（如 `signerTokenAccount`, `bankLiquidityVault`, `destinationTokenAccount`）。  
3️⃣ **mint 映射**：借助 `fact_token_balances` 将账户地址映射到对应的 Token mint。  
4️⃣ **标准化处理**：使用 `ez_asset_metadata` 获取 `decimals`，将 `raw_amount` 统一为标准单位。  
5️⃣ **价格换算**：联合 `ez_prices_hourly` 和 `fact_prices_ohlc_hourly`，获取小时级价格，换算为 USD。  

其中 TVL 和 Borrow Volume 使用了用户视角，剩余指标采用协议视角（核心账户为 `bankLiquidityVault`）。

---

### 特例说明：Protocol Fees & Liquidation Revenue

这两个指标采用多表联动链路计算：

1️⃣ 事件提取 → `lendingPoolCollectBankFees`  
2️⃣ 账户定位 → `feeVault` 或 `insuranceVault`  
3️⃣ 所有者确认 → `fact_token_account_owners`  
4️⃣ 转账匹配 → `fact_transfers`（以 `tx_id + owner` 识别 inflow）  
5️⃣ USD 价格换算 → 按转账时间获取小时价格，优先主表，fallback 副表

---

## 2. 数据源说明（Data Sources Used）

本项目使用 Flipside 提供的 Solana 核心链上表，按用途分为：

**Instruction 类：**  
- `solana.core.fact_decoded_instructions`：提取事件主表  
- `solana.core.fact_token_account_owners`：匹配 feeVault/insuranceVault 的 owner  

**资产类：**  
- `solana.core.fact_token_balances`：地址 ↔ mint 映射  
- `solana.price.ez_asset_metadata`：symbol、decimals 映射  

**价格类：**  
- `solana.price.ez_prices_hourly`：主价格源  
- `solana.price.fact_prices_ohlc_hourly`：close fallback  

**转账类：**  
- `solana.core.fact_transfers`：真实 inflow 流入转账记录

---

## 3. 关键假设说明（Key Assumptions / Fallback Strategy）

为保障指标计算的稳定性与行业一致性，本项目默认遵循以下规则：

1️⃣ **统一事件来源：** 所有行为统一提取自 `fact_decoded_instructions`。  
2️⃣ **流量类与收入类指标使用事件时价格：** 例如 Borrow Volume、Protocol Fees、Revenue。  
3️⃣ **余额类指标使用最新价格：** 如 TVL、Deposit Outstanding。  
4️⃣ **主价格源优先级：** 优先使用 `ez_prices_hourly`，若缺失 fallback 到 `ohlc_hourly.close`。  
5️⃣ **decimals 补全策略：** 缺失则按 symbol 匹配或链上校验补全。  
6️⃣ **价格颗粒度统一：** 所有价格 `DATE_TRUNC('hour', timestamp)`。  
7️⃣ **默认时间窗口：** 所有查询默认 30 天内，可手动修改。  
8️⃣ **空值处理：** 所有字段使用 `COALESCE()` 包裹处理。

---

## 4. 查询运行方式（How to Run/Test the Queries）

本项目查询可在 Flipside Studio（https://flipsidecrypto.xyz）中直接运行。

- **平台：** Flipside Studio  
- **数据范围：** 默认近 30 天，支持自定义扩展  
- **查询方式：** 在 Web UI 输入 SQL 运行  
- **输出字段：** 查询返回 `metric_name + usd_value` 格式  
- **可视化建议：** 支持接入 Metabase、Dune、Superset 等仪表板系统展示

---

> 本项目基于标准 SQL 编写，字段命名统一，结构模块化，欢迎社区复用或贡献。