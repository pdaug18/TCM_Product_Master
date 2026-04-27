# Inventory_Primary_Location_Flag Derivation — Matillion Implementation

## Business Rule

Flag indicating whether an item at a given location is a **primary manufacturing location**.

- **Purchased items (flag_source='P')**: Always `'N'` (not manufactured anywhere)
- **Manufactured items (flag_source='M')**:
  - **Location '10' (HQ)**: Always `'Y'` if item is manufactured there
  - **Other locations**: 
    - `'N'` if the item is ALSO manufactured at location '10' (location '10' takes precedence)
    - `'Y'` if location '10' is NOT a manufacturing location for this item

**Key Precedence Rule**: If an item is manufactured at location '10', ONLY location '10' is flagged as primary; all other manufacturing locations are `'N'`.

---

## Implementation Architecture (3-Component)

This derivation requires identifying items manufactured at location '10' first, then applying conditional logic per row.

### Component 1: IDENTIFY_HQ_MANUFACTURED_ITEMS
**Purpose**: Extract all items that are manufactured at location '10'  
**Input**: Main inventory master stream  
**Filter**: `flag_source = 'M' AND id_loc = '10'`  
**Output**: Distinct list of `id_item` values

```sql
SELECT DISTINCT
    TRIM(id_item) AS id_item
FROM [MAIN_INVENTORY_STREAM]
WHERE flag_source = 'M'
  AND TRIM(id_loc) = '10'
```

**Expected Output Columns**: `id_item` (item IDs manufactured at HQ)  
**Row Count Indicator**: Subset of total unique items (typically 20-40% of all items)

---

### Component 2: FLAG_HQ_MANUFACTURED_ITEMS
**Purpose**: Add a marker column to main stream indicating if item has HQ manufacturing  
**Input**: Main inventory master stream (left side); `IDENTIFY_HQ_MANUFACTURED_ITEMS` (right side)  
**Join Type**: LEFT JOIN on `TRIM(main.id_item) = TRIM(hq.id_item)`  
**Output**: Main stream with new column `Is_Manufactured_At_HQ` (Y/null)

```sql
LEFT JOIN IDENTIFY_HQ_MANUFACTURED_ITEMS hq
  ON TRIM(main.id_item) = TRIM(hq.id_item)
```

**Output**: New column `Is_Manufactured_At_HQ` (values: 'Y' or NULL)

---

### Component 3: APPLY_PRIMARY_LOCATION_FLAG_LOGIC
**Purpose**: Compute final `Inventory_Primary_Location_Flag` column using conditional logic  
**Input**: Output from Component 2 (stream with `Is_Manufactured_At_HQ` marker)  
**Logic**:
1. If `flag_source = 'P'` → `'N'` (purchased, not manufactured)
2. If `flag_source = 'M'` AND `id_loc = '10'` → `'Y'` (HQ manufacturing location)
3. If `flag_source = 'M'` AND `id_loc ≠ '10'` AND `Is_Manufactured_At_HQ = 'Y'` → `'N'` (secondary mfg location, HQ takes precedence)
4. If `flag_source = 'M'` AND `id_loc ≠ '10'` AND `Is_Manufactured_At_HQ IS NULL` → `'Y'` (only mfg location for item)
5. Otherwise → `'N'`

**SQL Expression**:

```sql
CASE
  WHEN flag_source = 'P' THEN 'N'
  WHEN flag_source = 'M' AND TRIM(id_loc) = '10' THEN 'Y'
  WHEN flag_source = 'M' AND TRIM(id_loc) != '10' AND Is_Manufactured_At_HQ = 'Y' THEN 'N'
  WHEN flag_source = 'M' AND TRIM(id_loc) != '10' AND Is_Manufactured_At_HQ IS NULL THEN 'Y'
  ELSE 'N'
END AS Inventory_Primary_Location_Flag
```

**Output Column**: `Inventory_Primary_Location_Flag` (values: always 'Y' or 'N', never null)

---

## Data Quality Notes

- **NULL Handling**: `Is_Manufactured_At_HQ` column will be NULL for items NOT manufactured at location '10'; CASE expression safely handles with explicit `IS NULL` check
- **Grain**: Item × Location (one row per combination); primary flag value depends on item's global manufacturing locations, not just current row's location
- **Idempotence**: Logic is deterministic — same input always produces same output
- **Completeness**: All rows receive a value ('Y' or 'N'); no nulls in final output

---

## Validation Checklist

- [ ] Component 1 executes with row count matching expected unique items at location '10' (typically 20-40% of total)
- [ ] Component 2 LEFT JOIN preserves all main stream rows (row count = main stream count)
- [ ] `Is_Manufactured_At_HQ` column shows 'Y' for items mfg'd at '10', NULL for others
- [ ] Component 3 output shows no NULL values in `Inventory_Primary_Location_Flag`
- [ ] Sample validation: Pick 3 items (1 only at loc 10, 1 only at loc 20, 1 at both 10 and 20) and verify flags match logic above
- [ ] QA_SUMMARY confirms 100% non-null rate for `Inventory_Primary_Location_Flag`

---

## Example Validation

For a test dataset with items A, B, C:

| Item | Location | flag_source | Is_Mfg_at_10 | Primary_Location_Flag |
|------|----------|-------------|--------------|----------------------|
| A    | 10       | M           | Y            | Y                    |
| A    | 20       | M           | Y            | N                    |
| B    | 20       | M           | NULL         | Y                    |
| C    | 10       | P           | N/A          | N                    |
| D    | 90       | M           | NULL         | Y                    |

**Interpretation**:
- Item A: Manufactured at both 10 & 20 → only location 10 flagged as primary
- Item B: Manufactured only at 20 → location 20 flagged as primary
- Item C: Purchased (not manufactured) → all locations marked 'N'
- Item D: Manufactured at 90 (not 10) → location 90 flagged as primary

---

## Integration Notes

- **Order of Execution**: Components must run in sequence (1 → 2 → 3)
- **Dependency**: Component 3 depends on output of Component 2
- **Insertion Point**: Add Component 1 as a parallel extract from ITMMAS_LOC_Bronze before main join layer
- **Final Projection**: Include `Inventory_Primary_Location_Flag` in output SELECT

