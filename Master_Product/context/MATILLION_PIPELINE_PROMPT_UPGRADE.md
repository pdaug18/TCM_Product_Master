# Matillion Pipeline Prompt: Master_Product_Table_upgrade Silver Build

## Project Overview

Build a production-ready Matillion orchestration + transformation pipeline to create **SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_UPGRADE** with a modular, debuggable architecture using Matillion components instead of raw SQL.

**Context:**
- This is a **Silver-layer master table** for product attributes (SKU-level enriched product data).
- All source tables are **Bronze-layer** enterprise system captures.
- Architecture mirrors the proven 7-layer approach from **ITEM_INVENTORY_MASTER** but tailored for product attributes, costs, vendor data, and hierarchical relationships.
- Pipeline must be **easy to debug by any engineer** — each source has isolated ingest, standardization, and dedup steps.
- Replace all raw SQL CTEs with appropriate Matillion components (Extract, Query, Filter, Join, Pivot, Aggregate, etc.).

---

## ⚠️ CRITICAL FINDINGS FROM CURRENT SQL IMPLEMENTATION

**Before building the pipeline, review these key business rules:**

1. **Code Commodity Filter: Exclude Parts**
   - ITMMAS_BASE filter: `CODE_COMM ≠ 'PAR'` for SKU attributes
   - Separate logic for Parent attributes: `CODE_COMM = 'PAR'` (parent-level only)
   - This separation ensures SKU and Parent attributes are handled independently

2. **Attribute Pivoting & Deduplication**
   - IM_CMCD_ATTR_VALUE table has multiple rows per (ID_ITEM, ID_ATTR)
   - Dedup strategy: Keep most recent by ROWID DESC per (CODE_COMM, ID_ITEM, ID_ATTR)
   - Then pivot using CASE WHEN logic: one column per attribute ID (ID_PARENT, CERT_NUM, COLOR, SIZE, etc.)
   - Use MAX() aggregation during pivot to collapse remaining rows

3. **Product Structure (Prop 65) Logic**
   - Join PRDSTR (product structure/BOM) to identify parent-component relationships
   - Filter components: Keep only those with `FLAG_STAT_ITEM = 'A'` (active) and `DATE_EFF_END > CURRENT_DATE()`
   - Dedup on DATE_EFF_END DESC, ROWID DESC to get latest effective record per component
   - Prop_65 flag = 'Y' if ANY component description contains 'PROP 65'

4. **Vendor Logic: Primary vs Secondary**
   - Primary Vendor: `FLAG_VND_PRIM = 'P'` — one record per item, provides ID_VND_ORDFM, ID_VND_PAYTO, ID_ITEM_VND, quote dates
   - Secondary Vendors: `FLAG_VND_PRIM = 'S'` — multiple records per item, aggregated using LISTAGG
   - Join VENMAS_PAYTO for vendor name lookups
   - **Important:** Left join vendors (nulls allowed if no vendor defined for item)

5. **Cost Data Deduplication**
   - ITMMAS_COST: One row per item expected, but use ROWID DESC to ensure latest
   - Columns include current, standard, and accumulated costs (labor, materials, FB, VB, etc.)
   - Handles versioning via DATE_CHG_COST_VA, DATE_ACCUM_COST, DATE_STD_COST

6. **Item Descriptions**
   - ITMMAS_DESCR: Multiple rows per item with different SEQ_DESCR values
   - For parents: Filter `SEQ_DESCR BETWEEN 800 AND 810`, use LISTAGG to concatenate
   - Dedup each (ID_ITEM, SEQ_DESCR) by ROWID DESC first
   - Child descriptions: Separate from parent descriptions

7. **Column Name Convention**
   - Final silver columns use **Title_Case_With_Underscores, no spaces**
   - All ID-type columns should be TRIM() at standardization stage to prevent hidden whitespace mismatches
   - Business-friendly descriptions: e.g., "Item_ID_Child_SKU" not "ID_ITEM"

---

## Source Tables: Detailed Specs

### 1. ITMMAS_BASE_Bronze (Item Master Base)
**Role:** Core item master data — SKU identifiers, status, descriptions, commodities.  
**Grain:** Item (ID_ITEM) — one row per item.  
**Join Key:** ID_ITEM  

**Key Columns:**
- ID_ITEM (TEXT(30), 0% null) — unique item identifier
- CODE_COMM (TEXT(3), no nulls) — commodity code ('PAR' for parents, others for SKUs)
- FLAG_STAT_ITEM (TEXT(1), no nulls) — status ('A' = active, 'O' = obsolete)
- DESCR_1, DESCR_2 (TEXT, various null %) — primary and secondary descriptions
- CODE_CAT_COST, CODE_CAT_PRDT — cost and product category codes
- Other audit fields: ID_USER_ADD, DATE_ADD, ID_USER_CHG, DATE_CHG

**Dedup Logic:**
- Business key: ID_ITEM
- Expected unique; if duplicates found, keep by ROWID DESC (most recent)
- Apply TRIM() to all ID fields at standardization

---

### 2. IM_CMCD_ATTR_VALUE_Bronze (Attribute Values — Pivotable)
**Role:** Flexible attributes storage — dimensions like COLOR, SIZE, TARIFF_CODE, etc.  
**Grain:** Item (ID_ITEM) x Attribute (ID_ATTR) — multiple rows per item across many attributes.  
**Join Key:** ID_ITEM, ID_ATTR (paired with CODE_COMM for separation)  

**Key Columns:**
- ID_ITEM, ID_ATTR, CODE_COMM (all TEXT, no nulls)
- VAL_STRING_ATTR (TEXT, varying null %) — attribute value
- ID_USER_ADD, DATE_ADD, ID_USER_CHG, DATE_CHG (audit)
- ROWID — version marker for dedup

**Critical Attributes to Extract (from current SQL):**
SKU Attributes (CODE_COMM ≠ 'PAR'):
  - ID_PARENT, CERT_NUM, COLOR, SIZE, LENGTH, TARIFF_CODE, UPC_CODE
  - PFAS, CLASS, PPC, PRICE_LIST, PRICE_LIST_DESC, PRICE_LIST_ID, PRICE_LIST_PT
  - PRIOR COMMODITY, RBN_WC, REASON, REPLACEMENT, REQUESTOR, SF_XREF

Parent Attributes (CODE_COMM = 'PAR'):
  - BERRY, CARE, HEAT TRANSFER, OTHER, PAD PRINT, PRODUCT LINE, PRODUCT TYPE
  - PRODUCT_APP, TRACKING, Z_BRAND, Z_CATEGORY, Z_GENDER

**Dedup Logic:**
- Business key: (CODE_COMM, ID_ITEM, ID_ATTR)
- Keep first by ROWID DESC (most recent record per attribute-item combo)
- Separate processing: SKU attributes vs Parent attributes (different CODE_COMM values)

**Pivot Strategy:**
- After dedup, pivot: GROUP BY (CODE_COMM, ID_ITEM) and use MAX(CASE WHEN ID_ATTR = 'X' THEN VAL_STRING_ATTR ELSE '' END)
- Result grain: One row per item per CODE_COMM with columns for each attribute

---

### 3. ITMMAS_DESCR_Bronze (Item Descriptions)
**Role:** Additional descriptions, potentially multiple per item.  
**Grain:** Item (ID_ITEM) x Description Sequence (SEQ_DESCR) — multiple rows per item.  
**Join Key:** ID_ITEM, SEQ_DESCR  

**Key Columns:**
- ID_ITEM (TEXT(30), 0% null)
- SEQ_DESCR (NUMBER, 0% null) — sequence number (800-810 for parent descriptions)
- DESCR_ADDL (TEXT, varying null %) — description text
- ROWID — version marker

**Dedup Logic:**
- Business key: (ID_ITEM, SEQ_DESCR)
- Keep first by ROWID DESC per combo
- **For Parent Descriptions:** Filter SEQ_DESCR BETWEEN 800 AND 810, then LISTAGG(DESCR_ADDL, '')
- **For Child Descriptions:** Concatenate all non-parent DESCR_1 + ' ' + DESCR_2 from ITMMAS_BASE

---

### 4. ITMMAS_COST_Bronze (Item Cost Data)
**Role:** Costing and hour standards — labor, materials, freight, variable/fixed burden.  
**Grain:** Item (ID_ITEM) — one row per item.  
**Join Key:** ID_ITEM  

**Key Columns (all numeric, mostly populated):**
- ID_ITEM, ID_LOC_HOME (for cost home location reference)
- Date fields: DATE_CHG_COST_VA, DATE_ACCUM_COST, DATE_STD_COST
- Hour fields: HR_LABOR_VA_CRNT, HR_MACH_VA_CRNT, etc.
- Current costs: COST_MATL_VA_CRNT, COST_LABOR_VA_CRNT, COST_FB_VA_CRNT, COST_VB_VA_CRNT, etc.
- Standard costs: HR_LABOR_VA_STD, COST_MATL_VA_STD, etc.
- Accumulated: HR_LABOR_ACCUM_CRNT, COST_MATL_ACCUM_CRNT, etc.

**Dedup Logic:**
- Business key: ID_ITEM
- Keep by ROWID DESC (most recent cost record)
- Apply TRIM() to ID fields

**Transformation:**
- Convert hours to minutes: HR_* * 60 → Minutes_* columns (for operational reporting)
- Pass all cost types through as-is (current, standard, accumulated)

---

### 5. ITMMAS_VND_Bronze (Item Vendor Relations)
**Role:** Vendor assignments — primary/secondary, order from, pay to, item numbers.  
**Grain:** Item (ID_ITEM) x Vendor (ID_VND_ORDFM, ID_VND_PAYTO) — multiple rows per item.  
**Join Key:** ID_ITEM, FLAG_VND_PRIM  

**Key Columns:**
- ID_ITEM (TEXT(30), 0% null)
- FLAG_VND_PRIM (TEXT(1), 0% null) — 'P' = primary, 'S' = secondary
- ID_VND_ORDFM, ID_VND_PAYTO, ID_ITEM_VND (TEXT, 0% null)
- DATE_QUOTE, DATE_EXPIRE_QUOTE (TIMESTAMP, varying null %)
- QTY_MULT_ORD, CODE_UM_VND (numeric/text, varying null %)

**Dedup Logic:**
- **Primary (FLAG_VND_PRIM = 'P'):** Keep first record per item (should be unique, but use ROWID DESC if duplicates)
- **Secondary (FLAG_VND_PRIM = 'S'):** Aggregate multiple records using LISTAGG on ID_VND_PAYTO and vendor names

**Join Strategy:**
- LEFT JOIN VENMAS_PAYTO on ID_VND_PAYTO to get vendor names (NAME_VND)
- Primary: Simple join for direct vendor columns
- Secondary: LISTAGG to concatenate multiple vendor IDs and names with ', ' separator

---

### 6. PRDSTR_Bronze (Product Structure / BOM)
**Role:** Parent-component relationships for Bill of Materials and Prop 65 compliance checking.  
**Grain:** Parent (ID_ITEM_PAR) x Component (ID_ITEM_COMP) — one or more rows per parent-component pair (versioning).  
**Join Key:** ID_ITEM_PAR  

**Key Columns:**
- ID_ITEM_PAR (TEXT(30), 0% null) — parent item ID
- ID_ITEM_COMP (TEXT(30), 0% null) — component item ID
- DATE_EFF_END (TIMESTAMP, 0% null) — effective end date
- ROWID — version marker

**Filter Criteria:**
- Join to ITMMAS_BASE on ID_ITEM_COMP: Keep only where `FLAG_STAT_ITEM = 'A'` (active components)
- Effective period: `DATE_EFF_END > CURRENT_DATE()` (only currently effective)

**Dedup Logic:**
- Business key: (ID_ITEM_PAR, ID_ITEM_COMP, DATE_EFF_END)
- Keep latest effective version: ROWID DESC per component-parent combo
- Result: Deduplicated BOM structure with active components only

**Prop 65 Derivation:**
- After dedup, for each distinct ID_ITEM_PAR, check if ANY component's ITMMAS_DESCR contains '%PROP 65%'
- Result: FLAG_PROP_65 = 'Y' if found, else 'N'

---

### 7. TABLES_CODE_CAT_COST_Bronze & TABLES_CODE_CAT_PRDT_Bronze (Lookup Tables)
**Role:** Reference data for cost categories and product verticals.  
**Grain:** Code (CODE_CAT_COST / CODE_CAT_PRDT) — one row per code.  
**Join Key:** CODE_CAT_COST / CODE_CAT_PRDT  

**Key Columns:**
- CODE_CAT_COST / CODE_CAT_PRDT (TEXT, 0% null) — code identifier
- DESCR (TEXT, 0% null) — human-readable description
- For TABLES_CODE_CAT_PRDT: ACCT_ID_SLS, ACCT_LOC_SLS, ACCT_DEPT_SLS, ACCT_ID_COGS, ACCT_LOC_COGS, ACCT_DEPT_COGS, ACCT_ID_INV, ACCT_LOC_INV

**No Dedup Needed:** Lookup tables are already unique on code.

---

## Pipeline Architecture

### Layer A: Source Ingestion & Standardization
Create one **Extract + Standardize** component per source:

1. **SRC_ITMMAS_BASE** → Extract from ITMMAS_BASE_Bronze
   - Select: ID_ITEM, DESCR_1, DESCR_2, CODE_CAT_COST, CODE_CAT_PRDT, CODE_COMM, FLAG_STAT_ITEM, RATIO_STK_PUR, WGT_ITEM, RATIO_STK_PRICE, CODE_UM_PRICE, CODE_UM_PUR, CODE_UM_STK, CODE_USER_*IM, TYPE_COST, ID_USER_ADD, DATE_ADD, ID_USER_CHG, DATE_CHG
   - Apply TRIM() to: ID_ITEM, ID_USER_ADD, ID_USER_CHG
   - Output: **STD_ITMMAS_BASE**

2. **SRC_IM_CMCD_ATTR_SKU** → Extract from IM_CMCD_ATTR_VALUE_Bronze (SKU branch)
   - Filter: CODE_COMM ≠ 'PAR'
   - Select: CODE_COMM, ID_ITEM, ID_ATTR, VAL_STRING_ATTR, ID_USER_ADD, DATE_ADD, ID_USER_CHG, DATE_CHG, ROWID
   - Apply TRIM() to: ID_ITEM, ID_ATTR, ID_USER_ADD, ID_USER_CHG
   - Output: **STD_IM_CMCD_ATTR_SKU**

3. **SRC_IM_CMCD_ATTR_PARENT** → Extract from IM_CMCD_ATTR_VALUE_Bronze (Parent branch)
   - Filter: CODE_COMM = 'PAR'
   - Select: CODE_COMM, ID_ITEM, ID_ATTR, VAL_STRING_ATTR, ID_USER_ADD, DATE_ADD, ID_USER_CHG, DATE_CHG, ROWID
   - Apply TRIM() to: ID_ITEM, ID_ATTR, ID_USER_ADD, ID_USER_CHG
   - Output: **STD_IM_CMCD_ATTR_PARENT**

4. **SRC_ITMMAS_DESCR** → Extract from ITMMAS_DESCR_Bronze
   - Select: ID_ITEM, SEQ_DESCR, DESCR_ADDL, ROWID
   - Apply TRIM() to: ID_ITEM
   - Output: **STD_ITMMAS_DESCR**

5. **SRC_ITMMAS_COST** → Extract from ITMMAS_COST_Bronze
   - Select all cost columns (see source table spec)
   - Apply TRIM() to: ID_ITEM, ID_LOC_HOME, ID_USER_CHG, ID_LOC_SRC_COST_STD
   - Output: **STD_ITMMAS_COST**

6. **SRC_ITMMAS_VND** → Extract from ITMMAS_VND_Bronze
   - Select: ID_ITEM, FLAG_VND_PRIM, ID_VND_PAYTO, ID_VND_ORDFM, ID_ITEM_VND, DATE_QUOTE, DATE_EXPIRE_QUOTE, QTY_MULT_ORD, CODE_UM_VND
   - Apply TRIM() to: ID_ITEM, ID_VND_PAYTO, ID_VND_ORDFM, ID_ITEM_VND
   - Output: **STD_ITMMAS_VND**

7. **SRC_PRDSTR** → Extract from PRDSTR_Bronze
   - Select: ID_ITEM_PAR, ID_ITEM_COMP, DATE_EFF_END, ROWID
   - Apply TRIM() to: ID_ITEM_PAR, ID_ITEM_COMP
   - Output: **STD_PRDSTR**

8. **SRC_TABLES_CODE_CAT_COST** → Extract from TABLES_CODE_CAT_COST_Bronze
   - Select: CODE_CAT_COST, DESCR
   - Output: **STD_TABLES_CODE_CAT_COST**

9. **SRC_TABLES_CODE_CAT_PRDT** → Extract from TABLES_CODE_CAT_PRDT_Bronze
   - Filter: CODE_TYPE_CUST IS NULL (product category only)
   - Select: CODE_CAT_PRDT, DESCR, ACCT_ID_SLS, ACCT_LOC_SLS, ACCT_DEPT_SLS, ACCT_ID_COGS, ACCT_LOC_COGS, ACCT_DEPT_COGS, ACCT_ID_INV, ACCT_LOC_INV
   - Apply TRIM() to: CODE_CAT_PRDT, ACCT_ID_SLS, ACCT_LOC_SLS, ACCT_ID_COGS, ACCT_LOC_COGS, ACCT_ID_INV, ACCT_LOC_INV
   - Output: **STD_TABLES_CODE_CAT_PRDT**

10. **SRC_VENMAS_PAYTO** → Extract from VENMAS_PAYTO_Bronze (for vendor name lookups)
    - Select: ID_VND, NAME_VND
    - Apply TRIM() to: ID_VND
    - Output: **STD_VENMAS_PAYTO**

---

### Layer B: Deduplication Components

1. **DEDUP_ITMMAS_BASE** ← STD_ITMMAS_BASE
   - Business key: ID_ITEM
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY ID_ITEM ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_ITMMAS_BASE**

2. **DEDUP_IM_CMCD_ATTR_SKU** ← STD_IM_CMCD_ATTR_SKU
   - Business key: (CODE_COMM, ID_ITEM, ID_ATTR)
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY CODE_COMM, ID_ITEM, ID_ATTR ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_IM_CMCD_ATTR_SKU**

3. **DEDUP_IM_CMCD_ATTR_PARENT** ← STD_IM_CMCD_ATTR_PARENT
   - Business key: (CODE_COMM, ID_ITEM, ID_ATTR)
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY CODE_COMM, ID_ITEM, ID_ATTR ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_IM_CMCD_ATTR_PARENT**

4. **DEDUP_ITMMAS_DESCR** ← STD_ITMMAS_DESCR
   - Business key: (ID_ITEM, SEQ_DESCR)
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY ID_ITEM, SEQ_DESCR ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_ITMMAS_DESCR**

5. **DEDUP_ITMMAS_COST** ← STD_ITMMAS_COST
   - Business key: ID_ITEM
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY ID_ITEM ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_ITMMAS_COST**

6. **DEDUP_ITMMAS_VND_PRIMARY** ← STD_ITMMAS_VND
   - Filter: FLAG_VND_PRIM = 'P'
   - Business key: ID_ITEM (one primary vendor per item expected)
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY ID_ITEM ORDER BY ROWID DESC) = 1
   - Output: **DEDUP_ITMMAS_VND_PRIMARY**

7. **DEDUP_ITMMAS_VND_SECONDARY** ← STD_ITMMAS_VND
   - Filter: FLAG_VND_PRIM = 'S'
   - No dedup needed; will aggregate in transform layer
   - Output: **DEDUP_ITMMAS_VND_SECONDARY**

8. **DEDUP_PRDSTR** ← STD_PRDSTR
   - Business key: (ID_ITEM_COMP)
   - Filter: Components must have FLAG_STAT_ITEM = 'A' in ITMMAS_BASE AND DATE_EFF_END > CURRENT_DATE
   - Dedup rule: ROW_NUMBER() OVER (PARTITION BY ID_ITEM_COMP ORDER BY DATE_EFF_END DESC, ROWID DESC) = 1
   - Output: **DEDUP_PRDSTR** (deduplicated BOM with active components only)

---

### Layer C: Data Pivots & Aggregations

1. **PIVOT_SKU_ATTRIBUTES** ← DEDUP_IM_CMCD_ATTR_SKU
   - Pivot all attributes into columns
   - GROUP BY ID_ITEM
   - Columns: ID_PARENT, CERT_NUM, COLOR, SIZE, LENGTH, TARIFF_CODE, UPC_CODE, PFAS, CLASS, PPC, PRICE_LIST, PRICE_LIST_DESC, PRICE_LIST_ID, PRICE_LIST_PT, PRIOR_COMMODITY, RBN_WC, REASON, REPLACEMENT, REQUESTOR, SF_XREF, EMPLOYEE_ID_USER_ADD, DATE_ADDED, EMPLOYEE_ID_USER_CHANGE, DATE_LAST_CHANGED
   - Use MAX(CASE WHEN ID_ATTR = 'X' THEN VAL_STRING_ATTR ELSE '' END) for each attribute
   - Output: **PIVOT_SKU_ATTRIBUTES** (one row per SKU with all attribute columns)

2. **PIVOT_PARENT_ATTRIBUTES** ← DEDUP_IM_CMCD_ATTR_PARENT
   - Pivot parent attributes into columns
   - GROUP BY ID_ITEM (parent ID)
   - Columns: BERRY, CARE, HEAT_TRANSFER, OTHER, PAD_PRINT, PRODUCT_LINE, PRODUCT_TYPE, PRODUCT_APP, TRACKING, Z_BRAND, Z_CATEGORY, Z_GENDER
   - Output: **PIVOT_PARENT_ATTRIBUTES** (one row per parent with attribute columns)

3. **AGG_CHILD_DESCRIPTIONS** ← DEDUP_ITMMAS_DESCR
   - Concatenate DESCR_ADDL values
   - GROUP BY ID_ITEM
   - LISTAGG(DESCR_ADDL, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS Description_Concatenated
   - Output: **AGG_CHILD_DESCRIPTIONS**

4. **AGG_PARENT_DESCRIPTIONS** ← DEDUP_ITMMAS_DESCR
   - Filter: SEQ_DESCR BETWEEN 800 AND 810
   - Concatenate DESCR_ADDL values
   - GROUP BY ID_ITEM
   - LISTAGG(DESCR_ADDL, '') WITHIN GROUP (ORDER BY SEQ_DESCR) AS Description_Parent
   - Output: **AGG_PARENT_DESCRIPTIONS**

5. **AGG_SECONDARY_VENDORS** ← DEDUP_ITMMAS_VND_SECONDARY LEFT JOIN STD_VENMAS_PAYTO
   - GROUP BY ID_ITEM
   - LISTAGG(VENMAS_PAYTO.NAME_VND, ', ') AS Secondary_Vendor_Names
   - LISTAGG(ID_VND_PAYTO, ', ') AS Secondary_Vendor_IDs
   - Output: **AGG_SECONDARY_VENDORS**

6. **DERIVE_PROP65** ← DEDUP_PRDSTR JOIN AGG_PARENT_DESCRIPTIONS
   - Extract distinct ID_ITEM_PAR values
   - Check if ANY component's description contains 'PROP 65' (case-insensitive)
   - GROUP BY ID_ITEM_PAR
   - CASE WHEN COUNT > 0 THEN 'Y' ELSE 'N' END AS Flag_Prop65
   - Output: **DERIVE_PROP65** (one row per parent, Prop65 flag)

---

### Layer D: Progressive Joins

1. **JOIN_BASE_WITH_COST** ← DEDUP_ITMMAS_BASE LEFT JOIN DEDUP_ITMMAS_COST
   - ON: DEDUP_ITMMAS_BASE.ID_ITEM = DEDUP_ITMMAS_COST.ID_ITEM
   - Add all cost columns + derived minutes columns
   - Output: **JOIN_BASE_WITH_COST**

2. **JOIN_ADD_SKU_ATTRIBUTES** ← JOIN_BASE_WITH_COST LEFT JOIN PIVOT_SKU_ATTRIBUTES
   - ON: JOIN_BASE_WITH_COST.ID_ITEM = PIVOT_SKU_ATTRIBUTES.ID_ITEM
   - Add all SKU attribute columns
   - Output: **JOIN_ADD_SKU_ATTRIBUTES**

3. **JOIN_ADD_VENDOR_PRIMARY** ← JOIN_ADD_SKU_ATTRIBUTES LEFT JOIN DEDUP_ITMMAS_VND_PRIMARY
   - ON: JOIN_ADD_SKU_ATTRIBUTES.ID_ITEM = DEDUP_ITMMAS_VND_PRIMARY.ID_ITEM
   - Add: ID_VND_ORDFM, ID_VND_PAYTO, ID_ITEM_VND, DATE_QUOTE, DATE_EXPIRE_QUOTE, QTY_MULT_ORD, CODE_UM_VND
   - Output: **JOIN_ADD_VENDOR_PRIMARY**

4. **JOIN_ADD_VENDOR_SECONDARY** ← JOIN_ADD_VENDOR_PRIMARY LEFT JOIN AGG_SECONDARY_VENDORS
   - ON: JOIN_ADD_VENDOR_PRIMARY.ID_ITEM = AGG_SECONDARY_VENDORS.ID_ITEM
   - Add: Secondary_Vendor_Names, Secondary_Vendor_IDs
   - Output: **JOIN_ADD_VENDOR_SECONDARY**

5. **JOIN_ADD_CODE_LOOKUPS** ← JOIN_ADD_VENDOR_SECONDARY LEFT JOIN STD_TABLES_CODE_CAT_COST
   - ON: JOIN_ADD_VENDOR_SECONDARY.CODE_CAT_COST = STD_TABLES_CODE_CAT_COST.CODE_CAT_COST
   - Add: CODE_CAT_COST_DESCR
   - Output: **JOIN_ADD_CODE_CAT_COST**

6. **JOIN_ADD_PRDT_LOOKUPS** ← JOIN_ADD_CODE_CAT_COST LEFT JOIN STD_TABLES_CODE_CAT_PRDT
   - ON: JOIN_ADD_CODE_CAT_COST.CODE_CAT_PRDT = STD_TABLES_CODE_CAT_PRDT.CODE_CAT_PRDT
   - Add: PRDT_DESCR, ACCT_ID_SLS, ACCT_LOC_SLS, ACCT_DEPT_SLS, ACCT_ID_COGS, ACCT_LOC_COGS, ACCT_DEPT_COGS, ACCT_ID_INV, ACCT_LOC_INV
   - Output: **JOIN_ADD_PRDT_LOOKUPS** (fully enriched with codes & accounting)

7. **JOIN_ADD_DESCRIPTIONS** ← JOIN_ADD_PRDT_LOOKUPS LEFT JOIN AGG_CHILD_DESCRIPTIONS
   - ON: JOIN_ADD_PRDT_LOOKUPS.ID_ITEM = AGG_CHILD_DESCRIPTIONS.ID_ITEM
   - Add: Description_Concatenated (child descriptions)
   - Output: **JOIN_ADD_DESCRIPTIONS**

8. **JOIN_ADD_PARENT_DATA** ← JOIN_ADD_DESCRIPTIONS LEFT JOIN PIVOT_PARENT_ATTRIBUTES
   - ON: JOIN_ADD_DESCRIPTIONS.ID_PARENT (from SKU attributes) = PIVOT_PARENT_ATTRIBUTES.ID_ITEM
   - Add all parent attribute columns
   - Add: Description_Parent (from AGG_PARENT_DESCRIPTIONS)
   - Output: **JOIN_ADD_PARENT_DATA**

9. **JOIN_ADD_PROP65** ← JOIN_ADD_PARENT_DATA LEFT JOIN DERIVE_PROP65
   - ON: JOIN_ADD_PARENT_DATA.ID_ITEM = DERIVE_PROP65.ID_ITEM_PAR
   - Add: Flag_Prop65 (default to 'N' if null)
   - Output: **JOIN_ADD_PROP65** (fully enriched dataset with all parent, cost, vendor, attribute data)

---

### Layer E: Business Transforms & Derivations

**XFM_BUSINESS_RULES** ← JOIN_ADD_PROP65

Apply derived business logic columns:

1. **Column Name Transformations** (apply Title_Case_With_Underscores convention):
   - ID_ITEM → Item_ID_Child_SKU
   - DESCR_1 || ' ' || DESCR_2 → Item_Description_Child_SKU
   - CODE_CAT_COST → Item_Cost_Category_ID
   - CODE_CAT_COST_DESCR → Item_Cost_Category (UPPER)
   - CODE_CAT_PRDT → Item_Vertical_Code
   - CODE_CAT_PRDT || ' - ' || PRDT_DESCR → Item_Vertical (UPPER + COALESCE to "Invalid Product Vertical" if null)
   - CODE_COMM → Item_Commodity_Code
   - RATIO_STK_PUR → Ratio_Purchase_to_Stock
   - And so on for all other columns...

2. **Accounting Mappings** (from CODE_CAT_PRDT lookups):
   - ACCT_ID_SLS → Item_Accounting_Sales_ID
   - ACCT_LOC_SLS → Item_Accounting_Sales_Location
   - ACCT_DEPT_SLS → Item_Accounting_Sales_Department
   - ACCT_ID_COGS → Item_Accounting_COGS_ID
   - ACCT_LOC_COGS → Item_Accounting_COGS_Location
   - ACCT_DEPT_COGS → Item_Accounting_COGS_Department
   - ACCT_ID_INV → Item_Accounting_Invoicing_ID
   - ACCT_LOC_INV → Item_Accounting_Invoicing_Location

3. **Cost-Derived Columns**:
   - Minutes_Labor_Current = HR_LABOR_VA_CRNT * 60
   - Minutes_Machine_Current = HR_MACH_VA_CRNT * 60
   - Minutes_Labor_Standard = HR_LABOR_VA_STD * 60
   - Minutes_Machine_Standard = HR_MACH_VA_STD * 60
   - Minutes_Labor_Accumulated_Current = HR_LABOR_ACCUM_CRNT * 60
   - Minutes_Machine_Accumulated_Current = HR_MACH_ACCUM_CRNT * 60
   - Minutes_Labor_Accumulated_Standard = HR_LABOR_ACCUM_STD * 60
   - Minutes_Machine_Accumulated_Standard = HR_MACH_ACCUM_STD * 60

4. **Null Handling**:
   - COALESCE(Cost_Category_Descr, 'INVALID COST CATEGORY') for code lookups
   - COALESCE(Vertical || ' - ' || Vertical_Descr, 'Invalid Product Vertical') for product vertical
   - Leave vendor and attribute fields null if no match (nulls are valid)

5. **Flag & Code Preservation**:
   - Keep all FLAG_* columns as-is (FLAG_STAT_ITEM, etc.)
   - UPPER() for code descriptions
   - Prop65 flag already 'Y' or 'N' (no null)

Output: **XFM_BUSINESS_RULES** (same row count as input, all business columns named and formatted per convention)

---

### Layer F: Data Quality Checks

**DQ_CHECKS** ← XFM_BUSINESS_RULES

**Critical Validation Rules:**

- **Null validation (FAIL if violated):**
  - Item_ID_Child_SKU must not be null (raise ERROR if found)
  - Raise ERROR and log details

- **Null warnings (LOG but do NOT fail):**
  - ID_PARENT (from SKU attributes): expected ~X% sparse (nulls normal)
  - Vendor fields: nulls expected if no vendor defined for item
  - Accounting fields: nulls expected if code lookup fails
  - Description fields: nulls expected for incomplete records

- **Duplicate key check:**
  - COUNT per Item_ID_Child_SKU should be 1
  - Raise WARNING if > 1 (dedup should have caught)

- **Code lookup coverage (WARN at thresholds):**
  - CODE_CAT_COST lookup match rate < 95% = WARN (expected ~98-100%)
  - CODE_CAT_PRDT lookup match rate < 95% = WARN (expected ~98-100%)
  - Log unmatched counts for debugging

- **Domain validation:**
  - FLAG_STAT_ITEM: domain should be ('A', 'O')
  - Validate against source column profile

- **Numeric validation:**
  - Cost fields: must be numeric (no text)
  - Hour/minute fields: no negative values expected (verify)

**Output:**
- **DQ_PASSED** (all rows that pass critical validations)
- **DQ_WARNINGS** (rows or aggregates that triggered warnings; logged for audit)
- Create **QA_SUMMARY** table with:
  - Total_Input_Rows, Total_QA_Passed, Total_QA_Failed, Total_Warnings
  - Null_Violations_Count (critical failures)
  - Duplicate_Violations_Count
  - Code_Lookup_Unmatched_Counts (CODE_CAT_COST, CODE_CAT_PRDT)
  - Sparse_Column_Null_Counts (ID_PARENT, vendor fields, etc.)
  - Timestamp of run

---

### Layer G: Final Projection to Silver

**OUT_MASTER_PRODUCT_TABLE** ← DQ_PASSED

- Project final business-ready columns (Title_Case_With_Underscores, no spaces)
- Order columns logically: Item IDs → Descriptions → Status → Attributes → Costs → Vendors → Accounting → Timestamps
- Write to target: **SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_UPGRADE** (Dynamic Table or Standard Table)
- Add load metadata:
  - Load_Timestamp = CURRENT_TIMESTAMP
  - Load_Source = 'Master_Product_Table_upgrade'

---

## Final Silver Column Mapping & Naming Convention

| Business Entity | Source Column(s) | Transform Rule | Silver Column Name | Data Type |
|---|---|---|---|---|
| **Item Identifiers** | | | | |
| Item Identity | ITMMAS_BASE.ID_ITEM | TRIM, pass-through | Item_ID_Child_SKU | TEXT |
| Item Descriptions | ITMMAS_BASE.DESCR_1 + DESCR_2 | Concatenate + TRIM | Item_Description_Child_SKU | TEXT |
| Parent Item ID | IM_CMCD_ATTR_VALUE.ID_PARENT | From pivoted SKU attributes | Item_ID_Parent_SKU | TEXT |
| **Status & Classification** | | | | |
| Child Item Status | ITMMAS_BASE.FLAG_STAT_ITEM | Pass-through | Item_Status_Child_Active_Status | TEXT |
| Commodity Code | ITMMAS_BASE.CODE_COMM | Pass-through | Item_Commodity_Code | TEXT |
| **Cost Category & Vertical** | | | | |
| Cost Category ID | ITMMAS_BASE.CODE_CAT_COST | TRIM, pass-through | Item_Cost_Category_ID | TEXT |
| Cost Category Description | TABLES_CODE_CAT_COST.DESCR | UPPER, COALESCE to 'INVALID' | Item_Cost_Category | TEXT |
| Vertical Code | ITMMAS_BASE.CODE_CAT_PRDT | UPPER, pass-through | Item_Vertical_Code | TEXT |
| Vertical Description | CODE_CAT_PRDT + DESCR | Concatenate + UPPER | Item_Vertical | TEXT |
| **Ratio & Weight** | | | | |
| Purchase to Stock Ratio | ITMMAS_BASE.RATIO_STK_PUR | Pass-through | Ratio_Purchase_to_Stock | NUMBER |
| Item Weight | ITMMAS_BASE.WGT_ITEM | Pass-through | Item_Weight | NUMBER |
| Stock to Price Ratio | ITMMAS_BASE.RATIO_STK_PRICE | Pass-through | Ratio_Stock_to_Price | NUMBER |
| **Unit of Measure** | | | | |
| Price UoM | ITMMAS_BASE.CODE_UM_PRICE | Pass-through | Item_Unit_of_Measure_Price | TEXT |
| Purchase UoM | ITMMAS_BASE.CODE_UM_PUR | Pass-through | Item_Unit_of_Measure_Purchase | TEXT |
| Stock UoM | ITMMAS_BASE.CODE_UM_STK | Pass-through | Item_Unit_of_Measure_Stock | TEXT |
| Vendor UoM | ITMMAS_VND.CODE_UM_VND | From primary vendor | Item_Unit_of_Measure_Vendor | TEXT |
| **Cost Data** | | | | |
| Hours Labor Current | ITMMAS_COST.HR_LABOR_VA_CRNT | Pass-through | Hours_Labor_Current | NUMBER |
| Hours Machine Current | ITMMAS_COST.HR_MACH_VA_CRNT | Pass-through | Hours_Machine_Current | NUMBER |
| Hours Labor Standard | ITMMAS_COST.HR_LABOR_VA_STD | Pass-through | Hours_Labor_Standard | NUMBER |
| Hours Machine Standard | ITMMAS_COST.HR_MACH_VA_STD | Pass-through | Hours_Machine_Standard | NUMBER |
| Hours Labor Accumulated Current | ITMMAS_COST.HR_LABOR_ACCUM_CRNT | Pass-through | Hours_Labor_Accumulated_Current | NUMBER |
| Hours Machine Accumulated Current | ITMMAS_COST.HR_MACH_ACCUM_CRNT | Pass-through | Hours_Machine_Accumulated_Current | NUMBER |
| Hours Labor Accumulated Standard | ITMMAS_COST.HR_LABOR_ACCUM_STD | Pass-through | Hours_Labor_Accumulated_Standard | NUMBER |
| Hours Machine Accumulated Standard | ITMMAS_COST.HR_MACH_ACCUM_STD | Pass-through | Hours_Machine_Accumulated_Standard | NUMBER |
| Minutes Labor Current | ITMMAS_COST.HR_LABOR_VA_CRNT | HR * 60 | Minutes_Labor_Current | NUMBER |
| Minutes Machine Current | ITMMAS_COST.HR_MACH_VA_CRNT | HR * 60 | Minutes_Machine_Current | NUMBER |
| Minutes Labor Standard | ITMMAS_COST.HR_LABOR_VA_STD | HR * 60 | Minutes_Labor_Standard | NUMBER |
| Minutes Machine Standard | ITMMAS_COST.HR_MACH_VA_STD | HR * 60 | Minutes_Machine_Standard | NUMBER |
| Minutes Labor Accumulated Current | ITMMAS_COST.HR_LABOR_ACCUM_CRNT | HR * 60 | Minutes_Labor_Accumulated_Current | NUMBER |
| Minutes Machine Accumulated Current | ITMMAS_COST.HR_MACH_ACCUM_CRNT | HR * 60 | Minutes_Machine_Accumulated_Current | NUMBER |
| Minutes Labor Accumulated Standard | ITMMAS_COST.HR_LABOR_ACCUM_STD | HR * 60 | Minutes_Labor_Accumulated_Standard | NUMBER |
| Minutes Machine Accumulated Standard | ITMMAS_COST.HR_MACH_ACCUM_STD | HR * 60 | Minutes_Machine_Accumulated_Standard | NUMBER |
| Cost Material Accumulated Current | ITMMAS_COST.COST_MATL_ACCUM_CRNT | Pass-through | Cost_Material_Accumulated_Current | NUMBER |
| Cost Material Accumulated Standard | ITMMAS_COST.COST_MATL_ACCUM_STD | Pass-through | Cost_Material_Accumulated_Standard | NUMBER |
| Cost Freight Current | ITMMAS_COST.COST_FB_VA_CRNT | Pass-through | Cost_Freight_Current | NUMBER |
| Cost Freight Standard | ITMMAS_COST.COST_FB_VA_STD | Pass-through | Cost_Freight_Standard | NUMBER |
| Cost Material Current | ITMMAS_COST.COST_MATL_VA_CRNT | Pass-through | Cost_Material_Current | NUMBER |
| Cost Material Standard | ITMMAS_COST.COST_MATL_VA_STD | Pass-through | Cost_Material_Standard | NUMBER |
| Cost Labor Current | ITMMAS_COST.COST_LABOR_VA_CRNT | Pass-through | Cost_Labor_Current | NUMBER |
| Cost Labor Standard | ITMMAS_COST.COST_LABOR_VA_STD | Pass-through | Cost_Labor_Standard | NUMBER |
| Cost Variable Burden Accumulated Current | ITMMAS_COST.COST_VB_ACCUM_CRNT | Pass-through | Cost_Variable_Burden_Accumulated_Current | NUMBER |
| Cost Variable Burden Accumulated Standard | ITMMAS_COST.COST_VB_ACCUM_STD | Pass-through | Cost_Variable_Burden_Accumulated_Standard | NUMBER |
| Cost Freight Accumulated Current | ITMMAS_COST.COST_FB_ACCUM_CRNT | Pass-through | Cost_Freight_Accumulated_Current | NUMBER |
| Cost Freight Accumulated Standard | ITMMAS_COST.COST_FB_ACCUM_STD | Pass-through | Cost_Freight_Accumulated_Standard | NUMBER |
| Cost Outside Service Current | ITMMAS_COST.COST_OUTP_VA_CRNT | Pass-through | Cost_Outside_Service_Current | NUMBER |
| **Vendor Data** | | | | |
| Primary Vendor Order From | ITMMAS_VND.ID_VND_ORDFM | From primary vendor | Item_Primary_Vendor_Order_From_ID | TEXT |
| Primary Vendor Pay To | ITMMAS_VND.ID_VND_PAYTO | From primary vendor | Item_Primary_Vendor_Pay_To_ID | TEXT |
| Primary Item Vendor Number | ITMMAS_VND.ID_ITEM_VND | From primary vendor | Item_Primary_Vendor_Item_Number | TEXT |
| Quote Date | ITMMAS_VND.DATE_QUOTE | From primary vendor | Item_Primary_Vendor_Quote_Date | TIMESTAMP |
| Quote Expiration Date | ITMMAS_VND.DATE_EXPIRE_QUOTE | From primary vendor | Item_Primary_Vendor_Quote_Expiration_Date | TIMESTAMP |
| Order Quantity Multiple | ITMMAS_VND.QTY_MULT_ORD | From primary vendor | Item_Order_Quantity_Multiple | NUMBER |
| Secondary Vendor Names | ITMMAS_VND (Flag='S') | LISTAGG names | Item_Secondary_Vendor_Names | TEXT |
| Secondary Vendor IDs | ITMMAS_VND (Flag='S') | LISTAGG IDs | Item_Secondary_Vendor_IDs | TEXT |
| **SKU Attributes (Pivoted)** | | | | |
| Parent ID | IM_CMCD_ATTR_VALUE.ID_PARENT | From pivoted attributes | Item_ID_Parent_SKU | TEXT |
| Certificate Number | IM_CMCD_ATTR_VALUE.CERT_NUM | From pivoted attributes | Item_Certificate_Number | TEXT |
| Color | IM_CMCD_ATTR_VALUE.COLOR | From pivoted attributes | Item_Color | TEXT |
| Size | IM_CMCD_ATTR_VALUE.SIZE | From pivoted attributes | Item_Size | TEXT |
| Length | IM_CMCD_ATTR_VALUE.LENGTH | From pivoted attributes | Item_Length | TEXT |
| Tariff Code | IM_CMCD_ATTR_VALUE.TARIFF_CODE | From pivoted attributes | Item_Tariff_Code | TEXT |
| UPC Code | IM_CMCD_ATTR_VALUE.UPC_CODE | From pivoted attributes | Item_UPC_Code | TEXT |
| PFAS | IM_CMCD_ATTR_VALUE.PFAS | From pivoted attributes | Item_PFAS | TEXT |
| Class | IM_CMCD_ATTR_VALUE.CLASS | From pivoted attributes | Item_Class | TEXT |
| PPC | IM_CMCD_ATTR_VALUE.PPC | From pivoted attributes | Item_PPC | TEXT |
| Price List | IM_CMCD_ATTR_VALUE.PRICE_LIST | From pivoted attributes | Item_Price_List | TEXT |
| Price List Description | IM_CMCD_ATTR_VALUE.PRICE_LIST_DESC | From pivoted attributes | Item_Price_List_Description | TEXT |
| Price List ID | IM_CMCD_ATTR_VALUE.PRICE_LIST_ID | From pivoted attributes | Item_Price_List_ID | TEXT |
| Price List PT | IM_CMCD_ATTR_VALUE.PRICE_LIST_PT | From pivoted attributes | Item_Price_List_PT | TEXT |
| Commodity Code Prior | IM_CMCD_ATTR_VALUE.PRIOR COMMODITY | From pivoted attributes | Item_Commodity_Code_Prior | TEXT |
| Work Center Rubin | IM_CMCD_ATTR_VALUE.RBN_WC | From pivoted attributes | Item_Work_Center_Rubin | TEXT |
| Status Obsolete Reason | IM_CMCD_ATTR_VALUE.REASON | From pivoted attributes | Item_Status_Obsolete_Reason | TEXT |
| Item Replaced By | IM_CMCD_ATTR_VALUE.REPLACEMENT | From pivoted attributes | Item_Replaced_By | TEXT |
| Status Obsolete Requestor | IM_CMCD_ATTR_VALUE.REQUESTOR | From pivoted attributes | Item_Status_Obsolete_Requestor | TEXT |
| SF Cross Reference | IM_CMCD_ATTR_VALUE.SF_XREF | From pivoted attributes | Item_SF_Xref | TEXT |
| **Parent Attributes (Pivoted)** | | | | |
| Parent Berry | IM_CMCD_ATTR_VALUE.BERRY (CODE_COMM='PAR') | From parent pivot | Item_Berry | TEXT |
| Parent Care | IM_CMCD_ATTR_VALUE.CARE | From parent pivot | Item_Care | TEXT |
| Parent Heat Transfer | IM_CMCD_ATTR_VALUE.HEAT TRANSFER | From parent pivot | Item_Heat_Transfer | TEXT |
| Parent Other | IM_CMCD_ATTR_VALUE.OTHER | From parent pivot | Item_Other | TEXT |
| Parent Pad Print | IM_CMCD_ATTR_VALUE.PAD PRINT | From parent pivot | Item_Pad_Print | TEXT |
| Parent Product Line | IM_CMCD_ATTR_VALUE.PRODUCT LINE | From parent pivot | Item_Product_Line | TEXT |
| Parent Product Type | IM_CMCD_ATTR_VALUE.PRODUCT TYPE | From parent pivot | Item_Product_Type | TEXT |
| Parent Product Application | IM_CMCD_ATTR_VALUE.PRODUCT_APP | From parent pivot | Item_Product_Application | TEXT |
| Parent Tracking | IM_CMCD_ATTR_VALUE.TRACKING | From parent pivot | Item_Tracking | TEXT |
| Parent Brand | IM_CMCD_ATTR_VALUE.Z_BRAND | From parent pivot | Item_Brand | TEXT |
| Parent Category | IM_CMCD_ATTR_VALUE.Z_CATEGORY | From parent pivot | Item_Product_Category | TEXT |
| Parent Gender | IM_CMCD_ATTR_VALUE.Z_GENDER | From parent pivot | Item_Gender | TEXT |
| **Descriptions** | | | | |
| Child Description | AGG_CHILD_DESCRIPTIONS | LISTAGG + concatenate | Item_Description_Child_SKU_Full | TEXT |
| Parent Description | AGG_PARENT_DESCRIPTIONS | LISTAGG (SEQ_DESCR 800-810) | Item_Description_Parent_SKU | TEXT |
| **Accounting** | | | | |
| Sales Account ID | TABLES_CODE_CAT_PRDT.ACCT_ID_SLS | TRIM, pass-through | Item_Accounting_Sales_ID | TEXT |
| Sales Location | TABLES_CODE_CAT_PRDT.ACCT_LOC_SLS | TRIM, pass-through | Item_Accounting_Sales_Location | TEXT |
| Sales Department | TABLES_CODE_CAT_PRDT.ACCT_DEPT_SLS | Pass-through | Item_Accounting_Sales_Department | TEXT |
| COGS Account ID | TABLES_CODE_CAT_PRDT.ACCT_ID_COGS | TRIM, pass-through | Item_Accounting_COGS_ID | TEXT |
| COGS Location | TABLES_CODE_CAT_PRDT.ACCT_LOC_COGS | TRIM, pass-through | Item_Accounting_COGS_Location | TEXT |
| COGS Department | TABLES_CODE_CAT_PRDT.ACCT_DEPT_COGS | Pass-through | Item_Accounting_COGS_Department | TEXT |
| Invoicing Account ID | TABLES_CODE_CAT_PRDT.ACCT_ID_INV | TRIM, pass-through | Item_Accounting_Invoicing_ID | TEXT |
| Invoicing Location | TABLES_CODE_CAT_PRDT.ACCT_LOC_INV | TRIM, pass-through | Item_Accounting_Invoicing_Location | TEXT |
| **Compliance & Flags** | | | | |
| Prop 65 Flag | DERIVE_PROP65 | 'Y' if component has 'PROP 65' in descr, else 'N' | Item_Flag_Prop65_Compliance | TEXT |
| User Add Employee ID | ITMMAS_BASE.ID_USER_ADD | TRIM, pass-through | Employee_ID_User_Add_ITMMAS_BASE | TEXT |
| Date Added | ITMMAS_BASE.DATE_ADD | Pass-through | Date_Added_ITMMAS_BASE | TIMESTAMP |
| User Change Employee ID | ITMMAS_BASE.ID_USER_CHG | TRIM, pass-through | Employee_ID_User_Change_ITMMAS_BASE | TEXT |
| Date Changed | ITMMAS_BASE.DATE_CHG | Pass-through | Date_Changed_ITMMAS_BASE | TIMESTAMP |
| **Load Metadata** | | | | |
| Load Timestamp | CURRENT_TIMESTAMP | System generated | Load_Timestamp | TIMESTAMP |

---

## Component Naming Convention

All components should follow this naming pattern:

- **SRC_[SourceTableName]**: Extract component from source table
- **STD_[SourceTableName]**: Standardized extract (TRIM, type cast, etc.)
- **DEDUP_[SourceTableName]**: Deduplicated (rowid-based dedup applied)
- **PIVOT_[Purpose]**: Attribute pivot components
- **AGG_[Purpose]**: Aggregation components
- **DERIVE_[Purpose]**: Derived/calculated columns
- **JOIN_[Purpose]**: Progressive join steps
- **XFM_[Purpose]**: Business transformation layer
- **DQ_[Check]**: Quality checks
- **OUT_[TableName]**: Final output to Silver layer

---

## Debuggability Best Practices

1. **Component Comments & Documentation:**
   - Each component should have a comment block explaining:
     - Purpose of the component
     - Business rule it enforces
     - Keys used (for joins, dedup)
     - Dedup strategy applied
     - Expected row count indicators

2. **Row Count Logging:**
   - After each major step, log:
     - Step name
     - Input row count
     - Output row count
     - Rows added/removed/filtered
     - Duplicate violations found

3. **Intermediate Output Inspection:**
   - Sample first 100 rows of key intermediates for inspection:
     - After dedup steps
     - After pivots
     - After major joins
   - Use Matillion's sample output feature

4. **Materialization Strategy:**
   - Materialize intermediate outputs during development/debugging (cache tables)
   - For production, disable materialization to optimize performance

5. **Error Handling:**
   - NULL key check: Raise ERROR if Item_ID_Child_SKU is NULL in final output
   - Duplicate key check: WARNING if business key count > 1
   - Code lookup mismatches: Log unmatched code values with counts

---

## Validation Checklist Before Production

- [ ] All 7 layers are implemented (Ingest, Standardize, Dedup, Pivot/Agg, Join, Transform, QA, Output)
- [ ] All sources have standardization (TRIM applied to ID fields)
- [ ] All sources have dedup logic with documented business keys
- [ ] Pivots produce correct grain (one row per item after pivoting attributes)
- [ ] All joins are LEFT JOINs (preserve all items from base)
- [ ] Row counts validated at each step (no unexpected data loss)
- [ ] Column names follow Title_Case_With_Underscores convention
- [ ] NULL handling is explicit (COALESCE for critical fields)
- [ ] Prop65 derivation validates with sample items (known Prop65 vs non-Prop65)
- [ ] Vendor aggregations produce comma-separated lists as expected
- [ ] Cost columns derived correctly (minutes calculations verified)
- [ ] QA_SUMMARY generated with all expected metrics
- [ ] No NULL values in Item_ID_Child_SKU (critical check)
- [ ] Final row count matches or exceeds expected baseline
- [ ] Sample rows spot-checked against source data for accuracy

---

## Performance Optimization Notes

1. **Use Matillion Caching:**
   - Cache intermediate pivot and aggregate results to avoid recomputation
   - Disable caching in production if not needed

2. **Filter Early:**
   - Apply CODE_CAT_PRDT filter (CODE_TYPE_CUST IS NULL) in SRC layer
   - Apply active component filter in SRC_PRDSTR layer (ITMMAS_BASE join with FLAG_STAT_ITEM check)

3. **Partition Strategy:**
   - For large tables (ITMMAS_BASE, IM_CMCD_ATTR_VALUE), consider partitioning by ID_ITEM if supported
   - Use Matillion's parallel processing where available

4. **Index Awareness:**
   - Ensure source tables have indexes on join keys (ID_ITEM, CODE_COMM, etc.)
   - Verify with DBA if needed

---

## Deliverables from Matillion Build

1. **Orchestration Job (Top-level):**
   - Shows workflow: Pre-checks → SRC Layer → STD Layer → DEDUP Layer → PIVOT/AGG Layer → JOIN Layer → XFM Layer → DQ Layer → Output
   - Error handling paths

2. **Transformation Job (Main logic):**
   - All components connected with visible data flow
   - Color-coded by layer (Ingest=Blue, Standardize=Green, Dedup=Yellow, Transforms=Orange, Output=Red, etc.)

3. **Component-Level Documentation:**
   ```
   | Component Name | Input(s) | Logic/Rule | Output | Row Count Indicator | Null Handling | Business Key |
   |---|---|---|---|---|---|---|
   | SRC_ITMMAS_BASE | ITMMAS_BASE_Bronze | TRIM IDs, select columns | STD_ITMMAS_BASE | ~X rows | All audit fields | ID_ITEM |
   | DEDUP_ITMMAS_BASE | STD_ITMMAS_BASE | Keep first by ROWID DESC | DEDUP_ITMMAS_BASE | Same as input | N/A | ID_ITEM |
   | PIVOT_SKU_ATTRIBUTES | DEDUP_IM_CMCD_ATTR_SKU | GROUP BY ID_ITEM, pivot ID_ATTR to columns | PIVOT_SKU_ATTRIBUTES | ~X unique items | Empty string for missing attributes | ID_ITEM |
   | ... | ... | ... | ... | ... | ... | ... |
   ```

4. **Data Quality & QA Summary:**
   - QA_SUMMARY table with all validation metrics
   - Warning/Error logs captured

---

## Next Steps

1. **Create SRC layer components** (Extract phase)
   - Materialize to verify correct source data
   - Validate row counts match expectations

2. **Create STD layer components** (Standardize phase)
   - Apply TRIM to all ID fields
   - Validate output row counts (should match SRC layer)

3. **Create DEDUP layer components** (Deduplication phase)
   - Test dedup logic with known duplicate scenarios
   - Log duplicate counts for audit

4. **Create PIVOT/AGG layer components** (Attribute pivoting & aggregation)
   - Test pivot logic with sample data
   - Verify one row per item post-pivot
   - Validate attribute column naming

5. **Create JOIN layer components** (Progressive enrichment)
   - Test each join incrementally
   - Verify join match rates
   - Validate NULL handling

6. **Create XFM layer component** (Business transforms)
   - Apply all column renamings
   - Validate calculations (e.g., minutes = hours * 60)
   - Apply UPPER() and COALESCE logic

7. **Create DQ layer component** (Quality checks)
   - Define all validation rules
   - Test error/warning generation
   - Create QA_SUMMARY

8. **Create OUT layer component** (Final output)
   - Project final column order
   - Write to SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE_UPGRADE
   - Add load metadata

9. **End-to-end test** (full pipeline validation)
   - Run full pipeline with sample data
   - Spot-check output against source
   - Validate row counts and data quality

10. **Production deployment** (hardening)
    - Disable intermediate materialization
    - Enable monitoring/alerting
    - Document SLA and recovery procedures
