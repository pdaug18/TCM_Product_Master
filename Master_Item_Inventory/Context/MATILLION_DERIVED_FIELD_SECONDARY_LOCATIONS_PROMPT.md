# Matillion Prompt: Derive Item_Secondary_Location_List

## Objective
Add the derived field `Item_Secondary_Location_List` to the Item_Inventory_Master pipeline. This field produces a comma-separated string of all locations where an item exists but is **not** its primary source location.

## Business Logic
**Secondary Location List** = A sorted, comma-separated list of `id_loc` values for each item where the item's `primary_source` does **not** match the short name of that location.

**LocationNames Lookup (hardcoded mapping):**

| id_loc | loc_name |
|--------|----------|
| 10     | CLE      |
| 20     | CHI      |
| 50     | ARK      |
| 90     | SAT      |
| 130    | DR       |

**Primary vs. Secondary Rule per row:**
- A row is **Primary** if `primary_source = loc_name` for that item-location pair.
- A row is **Secondary** (included in the list) if `primary_source ≠ loc_name`.

**Aggregation:** All secondary `id_loc` values for the same item are concatenated using `LISTAGG`, ordered ascending by `id_loc`.

**Output grain:** One row per item (aggregated), then joined back to the main item-location stream (one entry per item-location row).

**Example:**
- Item `ABC` exists at locations 10 (CLE), 20 (CHI), 50 (ARK)
- `primary_source` for item `ABC` is `'CHI'`
- Secondary locations = `'10, 50'` (id_loc values where loc_name ≠ 'CHI')
- The field `Item_Secondary_Location_List` = `'10, 50'` on ALL rows for item `ABC`

---

## Implementation Architecture (3-Component)

### Component 1: CALC_SECONDARY_LOCATION_FLAGS
**Component Name:** `CALC_SECONDARY_LOCATION_FLAGS`

**Purpose:** Add the `loc_name` (short name) to each item-location row by joining the hardcoded LocationNames mapping, then determine if each row is a primary or secondary location for that item.

**Input:** Main inventory master stream (post-primary-source derivation, which must include `primary_source` and `id_loc` columns).

**Logic:**
```sql
SELECT
    TRIM(id_item)   AS id_item,
    TRIM(id_loc)    AS id_loc,
    primary_source,
    CASE id_loc
        WHEN '10'  THEN 'CLE'
        WHEN '20'  THEN 'CHI'
        WHEN '50'  THEN 'ARK'
        WHEN '90'  THEN 'SAT'
        WHEN '130' THEN 'DR'
        ELSE NULL
    END AS loc_name,
    CASE
        WHEN primary_source = (
            CASE id_loc
                WHEN '10'  THEN 'CLE'
                WHEN '20'  THEN 'CHI'
                WHEN '50'  THEN 'ARK'
                WHEN '90'  THEN 'SAT'
                WHEN '130' THEN 'DR'
                ELSE NULL
            END
        ) THEN 'P'
        ELSE 'S'
    END AS location_match_flag
FROM [MAIN_INVENTORY_STREAM]
```

**Output Dataset:** `CALC_SECONDARY_LOCATION_FLAGS`

**Expected Columns:** `id_item`, `id_loc`, `primary_source`, `loc_name`, `location_match_flag`

---

### Component 2: AGG_SECONDARY_LOCATIONS_BY_ITEM
**Component Name:** `AGG_SECONDARY_LOCATIONS_BY_ITEM`

**Purpose:** Filter to secondary rows only and aggregate the `id_loc` values into a comma-separated list per item.

**Input:** `CALC_SECONDARY_LOCATION_FLAGS`

**Filter:** `location_match_flag = 'S'`

**Aggregation Logic:**
```sql
SELECT
    id_item,
    LISTAGG(id_loc, ', ') WITHIN GROUP (ORDER BY id_loc) AS Item_Secondary_Location_List
FROM CALC_SECONDARY_LOCATION_FLAGS
WHERE location_match_flag = 'S'
GROUP BY id_item
```

**Output Dataset:** `AGG_SECONDARY_LOCATIONS_BY_ITEM`

**Expected Grain:** One row per item (not per item-location).

**Data Types:**
- `id_item`: TEXT
- `Item_Secondary_Location_List`: TEXT (e.g., `'10, 50, 90'`)

**Null Handling:** Items where ALL locations are primary (i.e., they exist at only one location and it matches `primary_source`) will produce **no row** in this dataset. The subsequent LEFT JOIN will result in a NULL for those items — this is acceptable (see Validation section).

---

### Component 3: JOIN_SECONDARY_LOCATIONS_TO_MASTER
**Component Name:** `JOIN_SECONDARY_LOCATIONS_TO_MASTER`

**Purpose:** Join the aggregated secondary location list back to the main item-location stream.

**Left Side:** Main inventory master stream (full item-location grain)

**Right Side:** `AGG_SECONDARY_LOCATIONS_BY_ITEM`

**Join Type:** LEFT OUTER JOIN

**Join Condition:**
```sql
ON TRIM(main.id_item) = TRIM(agg_sec.id_item)
```

**Columns to Add:**
- `Item_Secondary_Location_List` (from `AGG_SECONDARY_LOCATIONS_BY_ITEM`)

**Output Dataset:** `JOIN_SECONDARY_LOCATIONS_TO_MASTER`

**Grain:** One row per (id_item, id_loc) — unchanged from main stream.

**Behavior:**
- All rows for the same item will carry the **same** `Item_Secondary_Location_List` value (it is item-level, not item-location-level).
- Items with only one location that matches primary source → `Item_Secondary_Location_List` = NULL (expected).

---

### Component 4: Final Projection
**Component Name:** `Master_Item_Inventory_Silver` (or update existing final projection component)

**Expression in SELECT:**
```sql
Item_Secondary_Location_List
```

**Output Column Name:** `Item_Secondary_Location_List`

**Data Type:** TEXT / VARCHAR

**NULL Handling Note:** Do NOT coalesce to empty string; leave as NULL when no secondary locations exist so downstream consumers can distinguish between "no secondary locations" and "unknown".

---

## Dependency — Required Upstream Column

`Item_Secondary_Location_List` depends on `primary_source` (also called `Item_Primary_Source_by_Location` in the silver output) being available in the main stream **before** Component 1 executes.

Ensure this component set is placed **after** the step that derives `primary_source` / `Item_Primary_Source_by_Location`.

---

## Pipeline Insertion Point

Insert these three components **after** the derivation of `primary_source` and **before** the final silver table write.

Suggested ordering in pipeline:
```
... → [DERIVE_PRIMARY_SOURCE] → CALC_SECONDARY_LOCATION_FLAGS
                                         ↓
                               AGG_SECONDARY_LOCATIONS_BY_ITEM
                                         ↓
[MAIN STREAM] → JOIN_SECONDARY_LOCATIONS_TO_MASTER → [FINAL SILVER WRITE]
```

---

## Validation

### Validation Checklist
- [ ] Component 1 row count = main stream row count (no filtering, just calculation)
- [ ] Component 2 row count ≤ total distinct items (one row per item that has at least one secondary location)
- [ ] Component 3 (JOIN) row count = main stream row count (LEFT JOIN must not add or drop rows)
- [ ] Items with exactly one location where it IS the primary source → `Item_Secondary_Location_List` is NULL
- [ ] Items with multiple locations → list contains all non-primary `id_loc` values, sorted ascending, comma-separated with space (e.g., `'10, 20'`)
- [ ] No item-location row should have its own `id_loc` appear in its own `Item_Secondary_Location_List` when that location IS the primary
- [ ] Row count before and after JOIN is identical

### Example Validation

| Item | Location | primary_source | loc_name | location_match_flag | Item_Secondary_Location_List |
|------|----------|----------------|----------|---------------------|------------------------------|
| A    | 10       | CLE            | CLE      | P                   | 20, 50                       |
| A    | 20       | CLE            | CHI      | S                   | 20, 50                       |
| A    | 50       | CLE            | ARK      | S                   | 20, 50                       |
| B    | 20       | CHI            | CHI      | P                   | NULL                         |
| C    | 10       | CLE            | CLE      | P                   | 90                           |
| C    | 90       | CLE            | SAT      | S                   | 90                           |

**Interpretation:**
- Item A: Primary source is CLE (loc 10); locations 20 and 50 are secondary → list = `'20, 50'` on all 3 rows
- Item B: Only exists at location 20 which IS its primary source → NULL (no secondary locations)
- Item C: Primary is CLE (loc 10); location 90 is secondary → list = `'90'` on all 2 rows

### QA Validation Query (Run After Pipeline Execution)
```sql
-- Check 1: Confirm no row count change from JOIN
SELECT COUNT(*) AS row_count FROM JOIN_SECONDARY_LOCATIONS_TO_MASTER;
-- Must equal row count from main stream before join

-- Check 2: Spot-check an item with multiple locations
SELECT
    id_item,
    id_loc,
    primary_source,
    Item_Secondary_Location_List
FROM SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER
WHERE id_item = '<test_item_id>'
ORDER BY id_loc;

-- Check 3: Confirm that all rows for same item have identical list value
SELECT
    id_item,
    COUNT(DISTINCT Item_Secondary_Location_List) AS distinct_list_values
FROM SILVER_DATA.TCM_SILVER.ITEM_INVENTORY_MASTER
GROUP BY id_item
HAVING COUNT(DISTINCT Item_Secondary_Location_List) > 1;
-- Expect 0 rows returned (all rows per item should have same list)
```

---

## Deliverables

1. Updated Matillion transformation graph showing:
   - `CALC_SECONDARY_LOCATION_FLAGS` component
   - `AGG_SECONDARY_LOCATIONS_BY_ITEM` component
   - `JOIN_SECONDARY_LOCATIONS_TO_MASTER` component
   - Final projection with `Item_Secondary_Location_List` column

2. Sample output rows showing:
   - Item ID
   - Inventory_Location_ID
   - Item_Secondary_Location_List (populated for multi-location items, NULL for single-location primaries)

3. QA validation output confirming:
   - Row count stable through JOIN
   - All rows for same item share identical `Item_Secondary_Location_List` value
   - No item's primary location appears in its own secondary list
