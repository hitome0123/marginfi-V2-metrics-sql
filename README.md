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
- [5. Challenges and Solutions](#5-challenges-and-solutions)
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
7️⃣ **Default date range:** Most queries default to full dataset days, editable in SQL.  
8️⃣ **Null-safe logic:** All numeric and metadata fields use `COALESCE()` for robust query behavior.

---

## 4. How to Run/Test the Queries

All SQL queries can be run directly on Flipside Studio (https://flipsidecrypto.xyz):

- **Platform:** Flipside Studio  
- **Data Range:** Default to the all days (modifiable)  
- **Run Interface:** Input SQL into Flipside Web UI  
- **Output Format:** `metric_name` + `usd_value` pairs
  
## 5. Challenges and Solutions

### ✅ Challenge 1: Unconventional protocol structure in MarginFi  
MarginFi adopts a custom lending architecture involving `bankLiquidityVault` and non-standard event types, differing from typical models like Aave or Compound.  
**Solution:** Analyzed decoded instructions via Flipside and aligned logic with MarginFi contracts to distinguish user vs protocol flows.

---

### ✅ Challenge 2: Missing or incomplete metadata (especially decimals)  
LP tokens and meme tokens like PumpFun often lack `decimals` in `ez_asset_metadata`.  
**Solution:** Used symbol-based heuristics (e.g., `%usd%` → 6 decimals) and manually patched key tokens with verified fallback values.

---

### ✅ Challenge 3: Incomplete price data from primary source  
Not all tokens had entries in `ez_prices_hourly`.  
**Solution:** Combined with `fact_prices_ohlc_hourly` using full outer join and fallback to `close` price to ensure price coverage.

---

### ✅ Challenge 4: Slow query performance on full-scale metrics  
TVL queries with large joins and normalization took over 200s.  
**Solution:** Added time windows (e.g., 30 days for borrow), pre-filtered tokens, modularized queries, and reduced join depth.

---

### ✅ Challenge 5: Differentiating protocol revenue vs protocol fees  
Fees logged via `lendingPoolCollectBankFees` don’t guarantee real inflow.  
**Solution:** Parsed vault addresses, verified owners via `fact_token_account_owners`, and matched transfers using `tx_id + owner`.

---

### ✅ Challenge 6: Event logs ≠ actual inflow  
Some fee events don’t move real funds.  
**Solution:** Only counted confirmed inflows to `feeVault` and `insuranceVault`, validated by token transfer records.

---

### ✅ Challenge 7: Outlier tokens distorted metrics  
Low-liquidity tokens without price data inflated TVL and revenue.  
**Solution:** Manually excluded tokens with <5 holders or no price feed.

---

### ✅ Challenge 8: Granularity mismatch between metric types  
Volume metrics require event-hour prices; snapshot metrics need latest prices.  
**Solution:** Applied separate logic: volume uses historical prices, TVL/outstanding use latest hourly prices.

---

### ✅ Challenge 9: Changing event structures  
Decoded instruction formats may change due to program upgrades.  
**Solution:** Used `LATERAL FLATTEN` + `account.name` to dynamically extract target fields like `signerTokenAccount`.

---

### ✅ Challenge 10: Multiple program_ids for MarginFi  
Old/test contracts pollute results.  
**Solution:** Restricted queries to the verified v2 program_id: `MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA`.

---

### ✅ Challenge 11: Overlapping account fields in events  
Same event may include `signerTokenAccount` and `destinationTokenAccount`.  
**Solution:** Defined per-metric account usage rules to avoid double-counting.

---

### ✅ Challenge 12: No clear distinction between user and protocol accounts  
Solana doesn’t differentiate EOAs from contract vaults.  
**Solution:** Used `fact_token_account_owners` to validate protocol-controlled token accounts only.


---

> All queries are modular and written in standard SQL with reusable logic. Contributions are welcome!
