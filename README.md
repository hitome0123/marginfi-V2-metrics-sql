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

# Challenges and Solutions

## **Challenge 1: MarginFi Protocol Structure Differs from Standard DeFi Protocols**
MarginFi adopts a unique lending architecture involving "bankLiquidityVault" and custom event types. To ensure accurate metrics, I thoroughly analyzed the **MarginFi protocol's architecture** and designed the metrics based on its unique structure.

**✅ Solution:**  
- I analyzed MarginFi's decoded instruction structure and referenced its architecture to ensure metrics reflect the actual operation of the protocol.

---

## **Challenge 2: Missing or Incomplete Metadata, Especially Decimals**
Some tokens, particularly LP tokens and meme coins like pumpfun, are missing decimals in `ez_asset_metadata`, or have ambiguous or missing symbols.

**✅ Solution:**  
- I implemented a fallback mechanism using symbol-based heuristics (e.g., `%usd% → 6 decimals`) and manually patched verified tokens to ensure normalization.

---

## **Challenge 3: Incomplete Price Data from Primary Source**
Although `ez_prices_hourly` is the primary price feed, it lacks price records for some tokens.

**✅ Solution:**  
- I joined it with `fact_prices_ohlc_hourly` table using a full outer join and fallback to close prices if the primary source is missing, ensuring full coverage in USD valuation.

---

## **Challenge 4: Slow Query Performance for Large-Scale Metrics like TVL**
TVL calculation involves large data scans, account flattening, multiple joins, and normalization, causing queries to run for over 200 seconds.

**✅ Solution:**  
- I pre-filtered token accounts, modularized subqueries, and performed aggregation before joins to improve performance.

---

## **Challenge 5: Differentiating Protocol Revenue vs. Protocol Fees**
While `lendingPoolCollectBankFees` event logs fee collection events, actual revenue includes real inflows to `feeVault` or `insuranceVault`, which must be validated separately.

**✅ Solution:**  
- I parsed vault addresses from decoded instructions, verified ownership via `fact_token_account_owners`, and matched transactions via `tx_id` + `owner` in `fact_transfers` table to differentiate protocol fees from liquidation revenue.

---

## **Challenge 6: Event-Based Data Does Not Guarantee Actual Inflow**
Not all fee events result in on-chain token transfers. Some events are logged but no funds move.

**✅ Solution:**  
- I ensured all fee/revenue metrics were based on verified inflows by linking `feeVault` addresses to actual transfers and validating owners.

---

## **Challenge 7: Outlier Tokens Skew Results with Unrealistic Valuation**
Some obscure tokens lack price data and have fewer than 5 holders. Including them distorts TVL and revenue numbers.

**✅ Solution:**  
- I **did not apply fallback** to these tokens, avoiding their impact on the final **USD valuation**. This ensures that illiquid or worthless tokens do not inflate the results.

---

## **Challenge 8: Inconsistent Price Granularity Across Different Metrics**
Volume-based metrics should use event-time prices, while snapshot-based metrics (like Outstanding) require the latest prices.

**✅ Solution:**  
- I implemented separate logic per metric type: Volume/Fees used “historical hourly prices”, while TVL/Outstanding used the most recent price, in line with industry practices.

---

## **Challenge 9: Changing Event Structure Over Time Due to Program Upgrades**
The decoded instruction structure of events can change (e.g., account ordering), which can break hardcoded SQL logic.

**✅ Solution:**  
- I used `LATERAL FLATTEN` and matched on `account.name` to dynamically identify target fields like `signerTokenAccount`, ensuring future-proof compatibility.

---

## **Challenge 10: Overlapping Account Fields Across Events May Cause Double-Counting**
Some events include `signerTokenAccount` and `destinationTokenAccount`. Misinterpreting these fields could lead to duplicated volume or TVL.

**✅ Solution:**  
- I defined per-metric field logic: TVL uses `signerTokenAccount`, Borrow Volume uses `destinationTokenAccount`, and each metric only counts its own flow.

---

## **Challenge 11: No Built-in Distinction Between User and Protocol Addresses**
Solana does not distinguish externally owned accounts (EOAs) from contracts, making it hard to separate user wallets from protocol vaults.

**✅ Solution:**  
- I validated token account owners using `fact_token_account_owners` table and only treated accounts owned by MarginFi’s program as protocol-level inflows/outflows.

---

## **Challenge 12: Overflow Risks in the Amount Parameter Inside `decoded_instruction` Field of `solana.core.fact_decoded_instructions`**
In Solana’s `solana.core.fact_decoded_instructions` table, the `decoded_instruction` JSON field sometimes contains an `amount` parameter with overflowed or abnormal values, caused by data inconsistencies or edge cases. These anomalies can significantly distort volume, TVL, or outstanding calculations if not properly handled.

**✅ Solution:**  
- I applied upper-bound filters when extracting the `amount` parameter, capping maximum values at reasonable thresholds (e.g., < 1e7 for mainstream tokens and < 1e8 for minor tokens) to exclude extreme outliers and maintain data integrity.

---

> All queries are modular and written in standard SQL with reusable logic. Contributions are welcome!
