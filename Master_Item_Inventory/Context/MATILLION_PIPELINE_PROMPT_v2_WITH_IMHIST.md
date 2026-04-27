# Matillion Pipeline Prompt: Item_Inventory_Master Silver Build

## Project Overview

Build a production-ready Matillion orchestration + transformation pipeline to create **SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER** with a clear, modular, debuggable architecture.

**Context:**
- This is a **Silver-layer master table** (deduplicated, business-rule-validated, business-ready).
- **Vendor-related logic removed** — no ITMMAS_VND_Bronze or VENMAS_PAYTO_Bronze in this model.
- **Vendor enrichment deferred to Gold layer** via join to Silver Vendor Master downstream.
- All source tables are **Bronze-layer** enterprise system captures.
- Pipeline must be **easy to debug by any engineer** — each source has isolated ingest, standardization, and dedup steps.

---

## ⚠️ CRITICAL FINDINGS FROM COLUMN PROFILES

**Review these before building the pipeline:**

1. **ITMMAS_REORD: ID_LOC is 100% NULL**
   - The column `ID_LOC` in ITMMAS_REORD_Bronze is completely null and should NOT be used.
   - **Use `ID_LOC_HOME` instead** (41 distinct, 0% null).
   - This is the location key for all joins with REORD.

2. **Negative Quantities Are Valid and Expected**
   - ITMMAS_LOC: QTY_ONHD and QTY_ONORD can be negative (reversals, corrections, returns).
   - IMHIST: QTY_ONHD_PRIOR and QTY_ONHD_CHG have negative ranges.
   - **Do NOT filter or reject negative quantities** — they represent legitimate business transactions.
   - Log but do not fail QA on negatives; warn only.

3. **SHPORD Status Filter: Only 'R' and 'S' Are Relevant**
   - STAT_REC_SO has 6 distinct values: 'A', 'C', 'E', 'R', 'U' (and possibly 'S').
   - The original script filters for `IN ('R', 'S')` — keep this filter to exclude archived/cancelled orders.

4. **Sparse Columns Should Be Skipped**
   - ITMMAS_LOC: CONCAT_KEY, DESCR, KEY_ALT, PRICE_* (100% null — no data to extract).
   - ITMMAS_REORD: DESCR, KEY_ALT, CSTM_* (96-100% null).
   - SHPORD: ID_ITEM_COMP, KYFILLER_*, KEY_BIN_* (100% null).
   - IMHIST: CODE_ABC (69.96% null), ID_CUST (97.29% null), KEY_BIN_* (78-96% null).

5. **Cardinality Insights for Join Coverage QA**
   - ITMMAS_LOC: 167,859 distinct items across 44 locations = ~3.8m potential item-locations.
   - SHPORD: 11,008 distinct items (only ~6.5% of all items have active shop orders).
   - REORD: 131,585 distinct items (78% of items have reorder parameters).
   - IMHIST: ~46k distinct items with transactional history (50-60% coverage expected).

---

## Source Tables: Detailed Specs

### 1. ITMMAS_LOC_Bronze (Inventory-by-Location Base)
**Role:** Primary grain driver — finished goods inventory at item-location level.  
**Grain:** Item (ID_ITEM) x Location (ID_LOC) — one row per item-location pair.  
**Join Key:** ID_ITEM, ID_LOC  
**Data Profile:**
- ID_ITEM: 167,859 distinct, 0% null, TEXT(30)
- ID_LOC: 44 distinct, 0.04% null (rare), TEXT(5)
- Quantities can be **negative** (reversals/corrections):
  - QTY_ONHD: Range -405,335 to 34,604,628
  - QTY_ONORD: Range -13 to 18,519,648
- Sparse columns (skip): CONCAT_KEY (100%), DESCR (100%), KEY_ALT (100%), PRICE_* (mostly 0.00 or empty)
- BIN_PRIM: 87.21% sparse (null for many items)

**Dedup Logic:** 
- Business key: (ID_ITEM, ID_LOC)
- Expected to be unique; profile shows no duplicates reported.
- If duplicates found: Keep first by DATE_ADD ASC (original record), raise WARNING with duplicate count.
- **Negative quantity handling:** Expected and valid (do NOT filter out). Log negative count in QA.

**Columns to Extract (minimum):**
  - ID_ITEM (TRIM at standardization stage)
  - ID_LOC (TRIM at standardization stage)
  - QTY_ONHD, QTY_ALLOC, QTY_ONORD (as-is, including negatives)
  - BIN_PRIM (nullable)
  - FLAG_SOURCE, FLAG_STK, FLAG_TRACK_BIN, FLAG_CNTRL, FLAG_FULFILL_TYPE, FLAG_PLCY_ORD
  - ID_PLANNER, TYPE_LOC, ID_RTE, FLAG_ISS_AUTO_SF

---

### 2. ITMMAS_BASE_Bronze (Item Master Filter)
**Role:** Finished-goods eligibility filter — controls item scope.  
**Grain:** Item (ID_ITEM) — one row per item.  
**Join Key:** ID_ITEM  
**Filter Criteria:**
- CODE_CAT_COST = '05' (Finished Goods marker)
- CODE_COMM ≠ 'PAR' (Exclude parts)
**Dedup Logic:**
- Business key: ID_ITEM
- Expect unique; if duplicates, keep first or raise alert.
- Columns to extract (minimum):
  - ID_ITEM (TRIM)

---

### 3. SHPORD_HDR_Bronze (Shop Order Pipeline Quantities)
**Role:** Manufacturing pipeline visibility — released/start/pending quantities per item-location.  
**Grain:** Item Parent (ID_ITEM_PAR) x Location (ID_LOC) — aggregated by manufacturing status.  
**Join Key:** ID_ITEM_PAR (join to FinishedGoods.ID_ITEM), ID_LOC  
**Data Profile:**
- ID_ITEM_PAR: 11,008 distinct, 0% null, TEXT(30)
- ID_LOC: 7 distinct, 0% null, TEXT(5)
- STAT_REC_SO: 6 distinct values: 'A', 'C', 'E', 'R', 'U', 0% null
  - 'R' = Released (relevant for manufacturing)
  - 'S' = Started (relevant for manufacturing) — **Note: 'S' not seen in profile but referenced in original logic**
  - 'A', 'C', 'E', 'U' = Other statuses (archived/cancelled/error/unknown — not relevant for inventory pipeline)
- QTY_ONORD: 251 distinct, 0 to 194,400 range (NO negatives, all positive)
- Sparse columns: ID_ITEM_COMP (100%), KYFILLER_* (100%), KEY_BIN_* (mostly 100%), custom fields (mostly 100%)

**Filter Criteria:**
- **STAT_REC_SO IN ('R', 'S')** — Only Released and Started statuses (per original script logic).
- This filter ensures only active manufacturing orders are included.

**Dedup Logic:**
- Business key: (ID_SO, SUFX_SO, ID_ITEM_PAR) — Unique shop order identifier (across locations and items).
- Expected: Many rows per item-location (multiple active shop orders).
- **Aggregate before dedup:** SUM(QTY_ONORD) by (ID_ITEM_PAR, ID_LOC), separately for each status + pending flag:
  - stat_rec_so = 'R' AND NOT ENDSWITH(id_item_par, '#') → Qty_Rel (released)
  - stat_rec_so = 'S' AND NOT ENDSWITH(id_item_par, '#') → Qty_Start (started)
  - stat_rec_so = 'R' AND ENDSWITH(id_item_par, '#') → Qty_Rel_PND (pending release)
  - stat_rec_so = 'S' AND ENDSWITH(id_item_par, '#') → Qty_Start_PND (pending start)
- **Pending suffix logic:** Items ending with '#' indicate pending SO not yet released to shop floor; separate tracking.
- Null handling: None expected for these columns; all are 0% null.

**Columns to Extract (post-aggregation):**
  - ID_ITEM_PAR (TRIM)
  - ID_LOC (TRIM)
  - Qty_Rel, Qty_Start, Qty_Rel_PND, Qty_Start_PND (all derived via SUM aggregation post-filter)

---

### 4. ITMMAS_REORD_Bronze (Reorder Policy Parameters)
**Role:** Replenishment controls — ROP, minimums, order multiples, lead times.  
**Grain:** Item (ID_ITEM) x Home Location (ID_LOC_HOME) — one row per item-home-location.  
**Join Key:** ID_ITEM, ID_LOC_HOME  
**⚠️ CRITICAL:** Column ID_LOC is 100% NULL in this table — **DO NOT USE**. Use ID_LOC_HOME instead.

**Data Profile:**
- ID_ITEM: 131,585 distinct, 0% null, TEXT(30)
- ID_LOC: **100% NULL** (useless column — ignore)
- ID_LOC_HOME: 41 distinct, 0% null, TEXT(5) — **This is the location key for joins**
- TYPE_REC: Constant value "40", 1 distinct, 0% null
- Sparse columns (skip): DESCR (100%), KEY_ALT (96.16%), CSTM_ALPHA_* (100%), CSTM_DATE_* (100%), CSTM_NUM_* (100%)
- Versioning columns: DATE_ADD, DATE_CHG (35.72% null), ID_USER_CHG (38.55% null)

**Dedup Logic:**
- Business key: (ID_ITEM, ID_LOC_HOME)
- Expected grain: One row per item-home-location.
- If duplicates found: Keep by DATE_CHG DESC (most recent change), then DATE_ADD DESC (most recent add); raise WARNING.
- Null handling: DATE_CHG 71.92% null in ID_USER_CHG column is expected (records without changes). Use DATE_ADD as tiebreaker if both are null.

**Columns to Extract (minimum):**
  - ID_ITEM (TRIM)
  - ID_LOC_HOME (TRIM) — **NOT ID_LOC**
  - LEVEL_ROP, QTY_MIN_ROP, QTY_MULT_ORD_ROP, QTY_ORD_ECON, LT_ROP

---

### 5. IMHIST_Bronze (ERP Historical Inventory Management Log)
**Role:** Historical inventory transaction context — supports trend analysis and audit trail.  
**Grain:** Item (ID_ITEM) x Location (ID_LOC) x Transaction Timestamp (DATE_CHG_QTY, TIME_CHG_QTY) x Sequence (SEQ_REC).  
This is a **transaction log**, not a point-in-time snapshot. Expect many rows per item-location.

**Key Columns:**
- ID_ITEM (TEXT(30), no nulls, 46,625 distinct) — Primary grain key
- ID_LOC (TEXT(5), no nulls, 43 distinct) — Primary grain key
- DATE_CHG_QTY (TIMESTAMP_NTZ, no nulls, range 2017-10-24 to 2026-04-07) — Transaction date
- TIME_CHG_QTY (NUMBER(6,0), no nulls, range 0 to 235959) — Transaction time (HHMMSS format)
- SEQ_REC (NUMBER(2,0), no nulls, range 0 to 21) — Sequence number per datetime for tiebreaking
- QTY_ONHD_PRIOR (NUMBER(8,0), no nulls) — Prior on-hand quantity (can be negative/reversed)
- QTY_ONHD_CHG (NUMBER(8,0), no nulls) — Change amount (signed)
- CODE_DTL (NUMBER(2,0), no nulls, 27 distinct) — Detail/transaction type code
- CODE_CAT_COST (TEXT(2), no nulls, 74 distinct) — Cost category
- CODE_ABC (TEXT(1), **69.96% null**, 4 distinct) — ABC classification (sparse)
- ID_BUYER (TEXT(2), ~0% null, 35 distinct) — Buyer ID
- FLAG_POST_HIST_IM (TEXT(1), no nulls, 2 distinct: 'N', 'P') — Post status flag
- COST_* fields (various NUMBER types, mostly populated) — Cost tracking metrics
- MEMO (TEXT(24), ~2% null, 838k+ distinct) — Free text memo (sparse)
- ID_CUST (TEXT(6), **97.29% null**, 349 distinct) — Customer ID (highly sparse)

**Join Key:** ID_ITEM, ID_LOC  
**Dedup Logic (CRITICAL):**
- This table is **transactional**; multiple rows per item-location are expected and correct.
- Business key: (ID_ITEM, ID_LOC, DATE_CHG_QTY, TIME_CHG_QTY, SEQ_REC) — forms unique transaction identifier.
- **Dedup Strategy:** 
  1. Filter out duplicate rows using rowversion + rowchecksum (Snowflake replication markers).
  2. Use QUALIFY ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY rowversion DESC) = 1.
  3. **For Inventory Master, extract ONLY the latest historical markers per item-location:**
     - Max(DATE_CHG_QTY) as Last_Inventory_Transaction_Date
     - Max(CODE_DTL) as Last_Transaction_Type_Code (or window partition by CODE_CAT_COST and pick relevant type)
     - Latest FLAG_POST_HIST_IM as Last_Post_Status_Flag
     - Consider aggregating total QTY_ONHD_CHG over a lookback window (e.g., last 90 days) if historical velocity is needed.
  4. Result grain for Inventory Master: One row per Item x Location (latest/summary metrics from IMHIST history).

**Columns to Extract (post-aggregation to item-location level):**
- ID_ITEM (TRIM)
- ID_LOC (TRIM)
- DT_Last_Inventory_Change = MAX(DATE_CHG_QTY)
- Code_Last_Transaction_Type = MODE(CODE_DTL) or MAX(CODE_DTL) (most recent detail code)
- Flag_Last_Post_Status = LAST_VALUE(FLAG_POST_HIST_IM) OVER (... ORDER BY DATE_CHG_QTY, TIME_CHG_QTY, SEQ_REC)
- *Optional:* QTY_Change_90_Days = SUM(QTY_ONHD_CHG) WHERE DATE_CHG_QTY >= CURRENT_DATE - 90

---

## Pipeline Architecture

### Layer A: Source Ingestion Components
Create one **Extract + Standardize** component per source table:

1. **SRC_ITMMAS_LOC** → reads from ITMMAS_LOC_Bronze
   - Select relevant columns
   - Apply TRIM() to all ID fields
   - Name output: **STD_ITMMAS_LOC**

2. **SRC_ITMMAS_BASE** → reads from ITMMAS_BASE_Bronze
   - Select ID_ITEM + filter columns (CODE_CAT_COST, CODE_COMM)
   - Apply TRIM() to ID_ITEM
   - Name output: **STD_ITMMAS_BASE**

3. **SRC_SHPORD_HDR** → reads from SHPORD_HDR_Bronze
   - Aggregate by ID_ITEM_PAR, ID_LOC, STAT_REC_SO, pending flag
   - Apply TRIM() to ID_ITEM_PAR, ID_LOC
   - Name output: **STD_SHPORD_AGG** (pre-pivoted: Qty_Rel, Qty_Start, Qty_Rel_PND, Qty_Start_PND)

4. **SRC_ITMMAS_REORD** → reads from ITMMAS_REORD_Bronze
   - Select relevant columns
   - Apply TRIM() to ID_ITEM, ID_LOC_HOME
   - Name output: **STD_ITMMAS_REORD**

5. **SRC_IMHIST** → reads from IMHIST_Bronze
   - Select relevant columns
   - Apply TRIM() to ID_ITEM, ID_LOC
   - Name output: **STD_IMHIST_RAW**

---

### Layer B: Dedup Components
Create one **Dedup** component per source:

1. **DEDUP_ITMMAS_LOC** ← SRC_ITMMAS_LOC
   - Business key: (ID_ITEM, ID_LOC)
   - Keep: FIRST or ROW_NUMBER() = 1 if duplicates exist
   - Log: Count duplicates found
   - Output: **DEDUP_ITMMAS_LOC**

2. **DEDUP_ITMMAS_BASE** ← SRC_ITMMAS_BASE
   - Business key: ID_ITEM
   - Keep: FIRST
   - Output: **DEDUP_ITMMAS_BASE**

3. **DEDUP_SHPORD_AGG** ← SRC_SHPORD_AGG
   - Business key: (ID_ITEM_PAR, ID_LOC)
   - Dedup already applied in SRC layer via aggregation
   - Light validation: ensure no nulls in join keys
   - Output: **DEDUP_SHPORD_AGG**

4. **DEDUP_ITMMAS_REORD** ← SRC_ITMMAS_REORD
   - Business key: (ID_ITEM, ID_LOC_HOME)
   - **Note:** Use ID_LOC_HOME as the location key, NOT ID_LOC (which is 100% null).
   - Keep: First by DATE_CHG DESC (most recent), then DATE_ADD DESC; raise WARNING if duplicates.
   - Output: **DEDUP_ITMMAS_REORD**

5. **DEDUP_IMHIST** ← SRC_IMHIST_RAW
   - Business key: (ID_ITEM, ID_LOC, DATE_CHG_QTY, TIME_CHG_QTY, SEQ_REC)
   - Remove duplicate transactions: QUALIFY ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY rowversion DESC) = 1
   - **Then aggregate to item-location grain:**
     - GROUP BY ID_ITEM, ID_LOC
     - DT_Last_Inventory_Change = MAX(DATE_CHG_QTY)
     - Code_Last_Transaction_Type = Mode or max of CODE_DTL
     - Flag_Last_Post_Status = Last value ordered by DATE_CHG_QTY DESC
   - Output: **DEDUP_IMHIST_ITEM_LOC** (now at item-location grain, one row per item-location)

---

### Layer C: FG Scope Filter
**FG_SCOPE_FILTER** ← DEDUP_ITMMAS_LOC INNER JOIN DEDUP_ITMMAS_BASE
- Join condition: DEDUP_ITMMAS_LOC.ID_ITEM = DEDUP_ITMMAS_BASE.ID_ITEM
- Enforce finished-goods scope: CODE_CAT_COST = '05' AND CODE_COMM ≠ 'PAR'
- Filter locations: ID_LOC IN ('10', '20', '50', '90', '130')
- Filter flag_source: FLAG_SOURCE IN ('P', 'M')
- Output: **FG_SCOPE_FILTERED** (scoped inventory-location rows only)

---

### Layer D: Progressive Joins
Build the enriched dataset through sequential LEFT JOINs, one per source:

1. **JOIN_SHPORD** ← FG_SCOPE_FILTERED LEFT JOIN DEDUP_SHPORD_AGG
   - ON: FG_SCOPE_FILTERED.ID_ITEM = DEDUP_SHPORD_AGG.ID_ITEM_PAR
     AND FG_SCOPE_FILTERED.ID_LOC = DEDUP_SHPORD_AGG.ID_LOC
   - Add: Qty_Rel, Qty_Start, Qty_Rel_PND, Qty_Start_PND (default to 0 if no match)
   - Output: **JOIN_SHPORD**

2. **JOIN_REORD** ← JOIN_SHPORD LEFT JOIN DEDUP_ITMMAS_REORD
   - ON: JOIN_SHPORD.ID_ITEM = DEDUP_ITMMAS_REORD.ID_ITEM
     AND JOIN_SHPORD.ID_LOC = DEDUP_ITMMAS_REORD.ID_LOC_HOME
   - **Important:** Join on ID_LOC_HOME from ITMMAS_REORD, not ID_LOC (which is 100% null).
   - Add: LEVEL_ROP, QTY_MIN_ROP, QTY_MULT_ORD_ROP, QTY_ORD_ECON, LT_ROP (nulls allowed)
   - Output: **JOIN_REORD**

3. **JOIN_IMHIST** ← JOIN_REORD LEFT JOIN DEDUP_IMHIST_ITEM_LOC
   - ON: JOIN_REORD.ID_ITEM = DEDUP_IMHIST_ITEM_LOC.ID_ITEM
     AND JOIN_REORD.ID_LOC = DEDUP_IMHIST_ITEM_LOC.ID_LOC
   - Add: DT_Last_Inventory_Change, Code_Last_Transaction_Type, Flag_Last_Post_Status (nulls allowed)
   - Output: **JOIN_IMHIST** (fully enriched dataset)

---

### Layer E: Business Transforms
**XFM_BUSINESS_RULES** ← JOIN_IMHIST
- Derive business-logic columns:
  - Flag_Source human label: CASE WHEN FLAG_SOURCE = 'M' THEN 'Manufactured' WHEN FLAG_SOURCE = 'P' THEN 'Purchased' ELSE 'Other' END
  - Qty_Cut logic: CASE WHEN FLAG_STK IN ('S', 'M') THEN Qty_Start + Qty_Start_PND ELSE 0 END (handle nulls with COALESCE)
  - Qty_Rel logic: CASE WHEN FLAG_STK IN ('S', 'M') THEN Qty_Rel + Qty_Rel_PND ELSE 0 END (handle nulls with COALESCE)
  - Add Location mapping (location code to name): '10' → 'CLE', '20' → 'CHI', etc.
- Output: **XFM_BUSINESS_RULES**

---

### Layer F: Data Quality Checks
**DQ_CHECKS** ← XFM_BUSINESS_RULES

**Critical Validation Rules:**
- **Null validation (FAIL if violated):**
  - ID_ITEM must not be null (raise ERROR if found)
  - ID_LOC must not be null (raise ERROR if found)
- **Null warnings (LOG but do NOT fail):**
  - QTY_ONHD, QTY_ALLOC, QTY_ONORD are numeric; nulls unexpected but do not fail.
  - BIN_PRIM: 87.21% sparse — nulls are normal; do not fail.
  - ID_USER_CHG in REORD source: 38.55% sparse — nulls are normal.
- **Negative quantity handling (WARN but do NOT fail):**
  - QTY_ONHD and QTY_ONORD can be negative (reversals/corrections valid).
  - Log negative counts per item; these represent legitimate business reversals.
  - SHPORD quantities must be positive (0 to 194,400 range); negative here = ERROR in source data.
- **Duplicate key check:**
  - COUNT per (ID_ITEM, ID_LOC) should be 1; raise WARNING if > 1 (dedup should have caught).
- **Join coverage (WARN at thresholds):**
  - SHPORD match rate < 30% = WARN (expected 30-40% of items have active SOs).
  - REORD match rate < 50% = WARN (expected 50-60% of items have reorder params).
  - IMHIST match rate < 70% = WARN (expected 70-80% of items have history).
  - Log unmatched counts for debugging.
- **Domain validation:**
  - FLAG_* columns: validate against expected domain (e.g., 'Y'/'N', 'S'/'M', 'C'/'N').
  - STAT_REC_SO (from SHPORD source): domain should be filtered to ('R', 'S') only.

**Output:**
- **DQ_PASSED** (all rows that pass critical validations)
- **DQ_WARNINGS** (rows or aggregates that triggered warnings; logged for audit)
- Create **QA_SUMMARY** table with:
  - Total_Input_Rows, Total_QA_Passed, Total_QA_Failed, Total_Warnings
  - Null_Violations_Count (critical failures)
  - Negative_Qty_Count (warnings, not failures)
  - Duplicate_Violations_Count
  - Join_Unmatched_Counts (by table: SHPORD, REORD, IMHIST)
  - Sparse_Column_Null_Counts
  - Timestamp of run

---

### Layer G: Final Projection to Silver
**OUT_ITEM_INVENTORY_MASTER** ← DQ_PASSED
- Project final business-ready columns (see below).
- Apply column naming convention: Title_Case with Underscore separation, no spaces.
- Write to target: **SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER** (Dynamic Table or Standard Table).
- Add load metadata:
  - Load_Timestamp = CURRENT_TIMESTAMP
  - Load_Source_Counts (optional): (SRC_ITMMAS_LOC_COUNT, SRC_SHPORD_COUNT, etc.)

---

## Final Silver Column Mapping

| Source Table/CTE | Source Column | Transform Rule | Silver Column Name |
|------------------|---------------|----------------|--------------------|
| STD_ITMMAS_LOC | ID_ITEM | TRIM, pass-through | Item_ID_Child_SKU |
| STD_ITMMAS_LOC | ID_LOC | TRIM, pass-through | Inventory_Location_ID |
| STD_ITMMAS_LOC | FLAG_SOURCE | CASE: 'M'→'Manufactured', 'P'→'Purchased' | Item_Sourcing_Type_Flag |
| STD_ITMMAS_LOC | QTY_ONHD | COALESCE(QTY_ONHD, 0) | Inventory_Quantity_On_Hand |
| STD_ITMMAS_LOC | QTY_ALLOC | COALESCE(QTY_ALLOC, 0) | Inventory_Quantity_Allocated |
| STD_ITMMAS_LOC | QTY_ONORD | COALESCE(QTY_ONORD, 0) | Inventory_Quantity_On_Order |
| STD_ITMMAS_LOC | BIN_PRIM | pass-through | Item_Primary_Bin_by_Location |
| STD_ITMMAS_LOC | FLAG_STK, FLAG_TRACK_BIN, FLAG_CNTRL, FLAG_FULFILL_TYPE, FLAG_PLCY_ORD | pass-through | Item_Stock_Flag, Item_Bin_Tracking, Item_Controlled_Noncontrolled_Flag, Item_Fulfillment_Type, Item_Order_Policy_Flag |
| STD_ITMMAS_LOC | ID_PLANNER, TYPE_LOC, ID_RTE, FLAG_ISS_AUTO_SF | pass-through | Item_Planned_Classification, Inventory_Location_Type, Item_Routing_Number, Item_Shop_Floor_Auto_Issue_Flag |
| DEDUP_ITMMAS_REORD | LEVEL_ROP, QTY_MIN_ROP, QTY_MULT_ORD_ROP, QTY_ORD_ECON, LT_ROP | pass-through (nulls ok) | Item_Inventory_Reorder_Point, Item_Inventory_Reorder_Point_Minimum, Item_Inventory_Reorder_Point_Mult, Item_Order_Quantity_Econ, Item_Inventory_Reorder_Point_Lead_Time |
| DEDUP_ITMMAS_REORD | ID_LOC_HOME | pass-through | Item_Home_Location_Code |
| DEDUP_SHPORD_AGG | Qty_Rel, Qty_Start, Qty_Rel_PND, Qty_Start_PND | SUM aggregates, pass-through | Inventory_Quantity_Released, Inventory_Quantity_Cut (derived), [internal] |
| XFM_BUSINESS_RULES | Qty_Cut, Qty_Rel (derived) | CASE logic above | Inventory_Quantity_Cut, Inventory_Quantity_Released |
| DEDUP_IMHIST_ITEM_LOC | DT_Last_Inventory_Change | pass-through | Inventory_History_Last_Change_Date |
| DEDUP_IMHIST_ITEM_LOC | Code_Last_Transaction_Type | pass-through | Inventory_History_Last_Transaction_Code |
| DEDUP_IMHIST_ITEM_LOC | Flag_Last_Post_Status | pass-through | Inventory_History_Last_Post_Status_Flag |

---

## Data Type & Format Validation

Based on column profile analysis:

- **ID fields:** Always TEXT; TRIM whitespace before joins to prevent hidden mismatches.
  - ID_ITEM: TEXT(30), 0% null, 167,859 distinct (ITMMAS_LOC) / 131,585 distinct (REORD) / 11,008 distinct (SHPORD)
  - ID_LOC: TEXT(5), 0.04% null, 44 distinct (ITMMAS_LOC) / 7 distinct (SHPORD)
  - ID_LOC_HOME: TEXT(5), 0% null, 41 distinct (REORD) — **Use this, NOT ID_LOC, for REORD joins**
- **Quantity fields:** Always numeric; handle negatives as valid.
  - QTY_ONHD: Range -405,335 to 34,604,628 (negatives valid)
  - QTY_ALLOC: Range 0 to 3,166,200 (non-negative)
  - QTY_ONORD: Range -13 to 18,519,648 (negatives valid)
  - COALESCE(qty, 0) for arithmetic to avoid null propagation.
- **Flag fields:** TEXT(1) with limited domain; validate against expected values.
  - FLAG_SOURCE: Expected domain ('M', 'P'); others = invalid.
  - FLAG_STK, FLAG_TRACK_BIN, FLAG_CNTRL, FLAG_FULFILL_TYPE, FLAG_PLCY_ORD: Domain-specific; validate per business rules.
- **Status field (SHPORD):** TEXT(1), domain: ('A', 'C', 'E', 'R', 'U').
  - **Only ('R', 'S') are relevant** for manufacturing pipeline (filter applied in SRC layer).
  - Note: 'S' not explicitly shown in profile but referenced in original logic; include in filter.
- **Date/Timestamp fields:** TIMESTAMP_NTZ.
  - Nulls allowed for some (e.g., DATE_CHG in REORD: 71.92% null, DATE_ACCTNG in IMHIST: 1.12% null).
  - Range validation: Ensure dates are within reasonable bounds (1993-2036 for ITMMAS, 2010-2999 for SHPORD).
- **Cost/Price fields:** NUMBER types; skip if all 0.00 or sparse (e.g., PRICE_SELL_* in ITMMAS_LOC: mostly 0.00).

---

- **Component Names:**
  - SRC_[TableName]: Source extract
  - STD_[TableName]: Standardized (trimmed, typed)
  - DEDUP_[TableName]: Deduplicated
  - XFM_[Purpose]: Business transforms
  - JOIN_[TableName]: Progressive joins
  - DQ_[Check]: Quality checks
  - OUT_[TableName]: Final output

- **Column Names:**
  - Title_Case_With_Underscores
  - No spaces
  - Descriptive but concise
  - Avoid cryptic codes (use full words)

---

## Debuggability Requirements

1. **Intermediate Materialization:**
   - Add optional DEBUG mode: if DEBUG_MODE = 'ON', materialize all intermediate outputs (STD_*, DEDUP_*, JOIN_*, XFM_*).
   - Disable for production; enable for troubleshooting.

2. **Row Count Logging:**
   - After each major step, log:
     - Step name
     - Input row count
     - Output row count
     - Rows added/removed/filtered
     - Null/duplicate violations found

3. **Comments:**
   - Add descriptive comments to each component explaining:
     - What it does
     - What business rule it enforces
     - What keys it uses
     - What dedup logic applies

4. **Sample Data:**
   - Expose first 100 rows of key intermediate outputs for inspection.

5. **Error Handling:**
   - NULL key check: raise ERROR if ID_ITEM or ID_LOC is NULL post-dedup.
   - Duplicate key check: raise WARNING if business key count > expected.
   - Join mismatch check: log unmatched counts per join.

---

## Data Quality Thresholds

Define QA pass/fail criteria:

- **Nulls in join keys:** FAIL if any NULL in ID_ITEM, ID_LOC post-standardization.
- **Duplicates on business key:** WARN if found; remove via dedup logic.
- **Negative quantities:** WARN if QTY_ONHD or QTY_ALLOC < 0 (valid for reversals, but log).
- **Join coverage:** 
  - SHPORD match rate < 40% = WARN (expected, as many items have no active SOs)
  - REORD match rate < 50% = WARN (expected, as many items may not have reorder params)
  - IMHIST match rate < 70% = WARN (most items should have history)

---

## Deliverables from Matillion

1. **Orchestration Job Graph**
   - Shows Pre-checks → Transformation Job → Post-validation → Success/Failure paths

2. **Transformation Job Graph**
   - Shows all 7 layers (Ingest, Standardize, Dedup, Filter, Joins, Transforms, QA, Output)
   - Visual connections between components

3. **Component-Level Summary Document:**
   ```
   | Component Name | Input(s) | Logic/Rule | Output | Row Count | Nulls Handled | Dedup Key |
   |---|---|---|---|---|---|---|
   | SRC_ITMMAS_LOC | ITMMAS_LOC_Bronze | TRIM IDs, select columns | STD_ITMMAS_LOC | ~Xm | n/a | n/a |
   | DEDUP_ITMMAS_LOC | STD_ITMMAS_LOC | Keep first per (ID_ITEM, ID_LOC) | DEDUP_ITMMAS_LOC | ~Xm | ID_ITEM, ID_LOC | (ID_ITEM, ID_LOC) |
   | ... | ... | ... | ... | ... | ... | ... |
   ```

4. **Dedup Logic Summary:**
   - Per-source dedup rules
   - Business keys used
   - Tie-break logic
   - Expected uniqueness

5. **Data Quality Rule Summary:**
   - All null checks
   - Duplicate checks
   - Join coverage thresholds
   - Pass/fail criteria

6. **Runbook:**
   - How to run the pipeline (job names, trigger schedule)
   - How to debug (enable DEBUG_MODE, check intermediate outputs, inspect QA_SUMMARY)
   - Common failure points:
     - Missing IMHIST rows (expected if new items added)
     - High shop-order unmatched rate (check ID_ITEM_PAR format vs ID_ITEM matching)
     - Reorder param nulls (expected for items without reorder settings)

---

## Success Criteria

✅ Another engineer can open Matillion, understand the flow, and trace any row from source to silver in under 15 minutes.  
✅ Every source table has isolated ingest + standardize + dedup + output components.  
✅ Null and duplicate handling is explicit and logged.  
✅ IMHIST aggregation to item-location grain is clear and documented.  
✅ All join keys are trimmed and typed consistently.  
✅ QA checks provide deterministic pass/fail with actionable error messages.  
✅ Final silver output matches column spec and naming convention.  
✅ Pipeline is modular enough to support future changes (e.g., add new columns from IMHIST without refactoring joins).

---

## Questions for Clarification (Before Matillion Build)

1. **IMHIST aggregation scope:** Should QTY_Change_90_Days and other historical metrics be included? Or just latest timestamps?
2. **Manufacturing locations ranking:** Is there a need to extract "primary mfg location" from FinishedGoods, or is that Gold-layer logic?
3. **Secondary locations derivation:** Should "secondary location list" be computed in silver, or deferred to Gold?
4. **Dynamic table vs standard table:** Confirm refresh cadence and target table type for ITEM_INVENTORY_MASTER.
5. **Location filter:** Confirm the hardcoded location list ('10', '20', '50', '90', '130') is still current; consider parameterizing if it changes.
6. **Cost fields from IMHIST:** Are any cost metrics needed in silver, or are these Gold-layer analytics?

