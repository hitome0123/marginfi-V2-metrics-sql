# 📊 MarginFi Protocol Metrics for Artemis Analytics Bounty

This project was built specifically for the [Artemis Analytics Data Analytics Bounty](https://earn.superteam.fun/listing/artemis-analytics-data-analytics-bounty/). All metrics are developed according to the official bounty task requirements.

It utilizes Solana on-chain data provided by Flipside to compute six core metrics related to the MarginFi protocol.

Metric designs follow standard practices in on-chain finance analytics, inspired by protocols like Aave. The metrics cover protocol-level deposits, borrowings, outstanding balances, and fee-based revenues, and are structured to support reuse and further development.

---

## 📑 Table of Contents

- [1. Metric Explanation](#1-metric-explanation)
- [2. Data Sources Used](#2-data-sources-used)
- [3. Key Assumptions / Fallback Strategy](#3-key-assumptions--fallback-strategy)
- [4. How to Run/Test the Queries](#4-how-to-runtest-the-queries)

---

## 1. Metric Explanation

This project includes 6 core metrics designed to analyze the asset dynamics and protocol revenues from both user and protocol perspectives.

All metrics follow an event-driven model with the following shared logic:

### General Framework:

1️⃣ **Event Extraction:** From `fact_decoded_instructions`, select key `event_type` values like `lendingAccountDeposit`, `lendingAccountBorrow`.  
2️⃣ **Account Extraction:** Parse target account fields from decoded instructions (e.g., `signerTokenAccount`, `bankLiquidityVault`, `destinationTokenAccount`).  
3️⃣ **Mint Mapping:** Use `fact_token_balances` to map token account addresses to mints.  
4️⃣ **Normalization:** Use `ez_asset_metadata` to obtain `decimals` and normalize raw amounts.  
5️⃣ **Price Matching:** Combine `ez_prices_hourly` and `fact_prices_ohlc_hourly` to obtain hourly price snapshots and convert to USD.  

TVL and Borrow Volume are computed from the user’s perspective, using user deposit/withdraw accounts. All other metrics use protocol-level addresses like `bankLiquidityVault`.

---

### Special Case: Protocol Fees & Liquidation Revenue

These metrics are calculated through multi-table joins instead of direct price * amount logic:

1️⃣ Event filtering via `lendingPoolCollectBankFees`  
2️⃣ Locate `feeVault` or `insuranceVault` account from `decoded_instruction.accounts`  
3️⃣ Identify the owner using `fact_token_account_owners`  
4️⃣ Match the token inflow using `fact_transfers` (by `tx_id + owner`)  
5️⃣ Use the timestamped price to convert inflow to USD (prefer primary, fallback to secondary)

---

## 2. Data Sources Used

The following core Solana datasets from Flipside are used:

**Instruction Tables:**  
- `solana.core.fact_decoded_instructions`: Main source for protocol-level events  
- `solana.core.fact_token_account_owners`: Maps feeVault/insuranceVault to receiving accounts  

**Token & Metadata:**  
- `solana.core.fact_token_balances`: Maps token accounts to mints  
- `solana.price.ez_asset_metadata`: Provides symbol and decimals  

**Price Feeds:**  
- `solana.price.ez_prices_hourly`: Primary price feed (hourly granularity)  
- `solana.price.fact_prices_ohlc_hourly`: Fallback OHLC data  

**Transfer Records:**  
- `solana.core.fact_transfers`: Tracks real inflow transactions for protocol revenue

---

## 3. Key Assumptions / Fallback Strategy

To ensure stability and consistency with industry practices, this project adopts the following assumptions:

1️⃣ **Unified event source:** All protocol actions are derived from `fact_decoded_instructions`.  
2️⃣ **Use event-time prices for flow/revenue metrics:** Metrics like `Borrow Volume`, `Protocol Fees` are computed using prices at the time of transaction.  
3️⃣ **Use latest prices for snapshot metrics:** Metrics like `TVL`, `Borrow Outstanding`, `Deposit Outstanding` use the most recent hourly price.  
4️⃣ **Primary price source preference:** Prefer `ez_prices_hourly`, fallback to `fact_prices_ohlc_hourly.close` if unavailable.  
5️⃣ **Decimals fallback:** If decimals are missing, infer via symbol heuristics (e.g. `*usd` = 6), or verified via chain lookup.  
6️⃣ **Hour-based price granularity:** All prices are truncated to the hour using `DATE_TRUNC('hour', timestamp)`.  
7️⃣ **Default date range:** All queries default to 30 days, editable in SQL.  
8️⃣ **Null-safe logic:** All numeric and metadata fields use `COALESCE()` for robust query behavior.

---

## 4. How to Run/Test the Queries

All SQL queries can be run directly on Flipside Studio (https://flipsidecrypto.xyz):

- **Platform:** Flipside Studio  
- **Data Range:** Default to the past 30 days (modifiable)  
- **Run Interface:** Input SQL into Flipside Web UI  
- **Output Format:** `metric_name` + `usd_value` pairs  
- **Visualization:** Supports export to dashboards like Metabase, Superset, or Dune

---

> All queries are modular and written in standard SQL with reusable logic. Contributions are welcome!
