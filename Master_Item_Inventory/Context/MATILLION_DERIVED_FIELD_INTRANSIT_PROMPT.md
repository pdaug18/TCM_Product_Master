# Matillion Prompt: Derive Inventory_Quantity_In_Transit

## Objective
Add the derived field `Inventory_Quantity_In_Transit` to the Item_Inventory_Master pipeline. This field captures all quantities currently in-transit for each item.

## Business Logic
**In-Transit Inventory** = Sum of all on-hand quantities (QTY_ONHD) for each item at location ID_LOC = 'INTR'.

**Rationale:** The ITMMAS_LOC_Bronze table tracks inventory by item and location. Location 'INTR' is reserved for inventory currently in transit. By aggregating quantities at this location, we capture the in-transit inventory for each item across the entire enterprise, regardless of source or destination location.

---

## Implementation

### Component 1: Extract and Filter In-Transit Inventory
**Component Name:** `CALC_INTRANSIT_INVENTORY`

**Source Table:** BRONZE_DATA.TCM_BRONZE."ITMMAS_LOC_Bronze"

**Filter Criteria:**
```sql
WHERE TRIM(ID_LOC) = 'INTR'
```

**Columns to Select:**
- TRIM(ID_ITEM) AS id_item
- QTY_ONHD

**Output Table/Dataset:** `STAGING_INTRANSIT_INVENTORY`

---

### Component 2: Aggregate to Item Level
**Component Name:** `AGG_INTRANSIT_BY_ITEM`

**Source:** STAGING_INTRANSIT_INVENTORY

**Aggregation Logic:**
```sql
SELECT
    id_item,
    SUM(COALESCE(qty_onhd, 0)) AS Inventory_Quantity_In_Transit
FROM STAGING_INTRANSIT_INVENTORY
GROUP BY id_item
```

**Output Table/Dataset:** `AGG_INTRANSIT_BY_ITEM`

**Expected Grain:** One row per item (not per item-location).

**Data Type:** 
- `id_item`: TEXT
- `Inventory_Quantity_In_Transit`: NUMBER(18,2)

---

### Component 3: Join to Main Item-Location Stream
**Component Name:** `JOIN_INTRANSIT_TO_MASTER`

**Source:** Main item-location stream (e.g., JOIN_REORD or current final dataset)

**Join Target:** AGG_INTRANSIT_BY_ITEM

**Join Condition:**
```sql
ON TRIM(main_stream.id_item) = TRIM(agg_intransit.id_item)
```

**Join Type:** LEFT OUTER JOIN

**Columns to Add:**
- `Inventory_Quantity_In_Transit` (from AGG_INTRANSIT_BY_ITEM)

**Output:** `JOIN_INTRANSIT_TO_MASTER`

**Null Handling:** If an item has no in-transit inventory, result will be NULL; coalesce to 0 in final projection.

---

### Component 4: Final Projection
**Component Name:** `Master_Inventory_Master` (or update existing final projection)

**Expression in SELECT:**
```sql
COALESCE(Inventory_Quantity_In_Transit, 0) AS Inventory_Quantity_In_Transit
```

**Output Column Name:** `Inventory_Quantity_In_Transit`

---

## Validation

1. **Null Safety:**
   - No item-location row should have a null Inventory_Quantity_In_Transit in final output; default to 0.

2. **Grain Check:**
   - Final table grain = one row per (ID_ITEM, ID_LOC).
   - Inventory_Quantity_In_Transit will be the same for all rows of the same item (aggregated at item level).
   - Example: Item ABC with quantities at locations 10, 20, and in-transit will have:
     - Row 1: Item=ABC, Loc=10, Inventory_Quantity_In_Transit=<summed qty at INTR>
     - Row 2: Item=ABC, Loc=20, Inventory_Quantity_In_Transit=<same summed qty at INTR>

3. **Coverage Check:**
   - Items with no in-transit inventory = 0 (not NULL).
   - Items with in-transit inventory = positive number.

4. **Row Count Stability:**
   - Row count before and after in-transit join should remain stable (LEFT JOIN; no rows added/removed).
   - Sample output to QA log showing Inventory_Quantity_In_Transit populated and non-null in final table.

---

## Deliverables

1. Updated Matillion transformation graph showing:
   - CALC_INTRANSIT_INVENTORY component
   - AGG_INTRANSIT_BY_ITEM component
   - JOIN_INTRANSIT_TO_MASTER component
   - Final projection with Inventory_Quantity_In_Transit column

2. Sample output rows showing:
   - Item ID
   - Inventory Location ID
   - Inventory_Quantity_In_Transit (non-null values)

3. QA validation output confirming:
   - No nulls in Inventory_Quantity_In_Transit in final output
   - Row count stable
   - In-transit quantities match source aggregation

