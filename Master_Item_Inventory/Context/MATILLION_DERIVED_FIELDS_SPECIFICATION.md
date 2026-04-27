# Inventory Master Derived Fields — Complete Specification

This document defines all 8 derived fields for the Item_Inventory_Master silver table, with exact SQL expressions for Matillion implementation.

---

## Derived Field 1: Inventory_Quantity_In_Transit

**Purpose**: Total on-hand quantity at the 'INTR' (in-transit) location for each item, aggregated to item level.

**Business Logic**: 
- Extract all inventory records where `id_loc = 'INTR'`
- Sum quantities by `id_item`
- Join back to master stream (left join; repeats value for all locations of same item)

**SQL Expression**:
```sql
COALESCE(Inventory_Quantity_In_Transit, 0) AS Inventory_Quantity_In_Transit
```

**Implementation**: Refer to `MATILLION_DERIVED_FIELD_INTRANSIT_PROMPT.md` (3-component architecture: extract INTR → aggregate by item → LEFT JOIN to master)

**Data Type**: NUMBER  
**Nullability**: Non-null (always 0 or positive value)

---

## Derived Field 2: Inventory_Primary_Location_Flag

**Purpose**: Flag indicating if an item at a given location is a primary manufacturing location (vs secondary or purchased).

**Business Logic**:
- If `flag_source = 'P'` (Purchased) → Always `'N'`
- If `flag_source = 'M'` (Manufactured):
  - If location = '10' (HQ) → `'Y'`
  - If location ≠ '10' AND item is also mfg'd at location '10' → `'N'` (HQ takes precedence)
  - If location ≠ '10' AND item is NOT mfg'd at location '10' → `'Y'` (only mfg location)

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

**Implementation**: Refer to `MATILLION_DERIVED_FIELD_PRIMARY_LOCATION_PROMPT.md` (3-component architecture: identify HQ items → LEFT JOIN marker → apply CASE logic)

**Data Type**: TEXT (fixed 1 character: 'Y' or 'N')  
**Nullability**: Non-null (always 'Y' or 'N')

---

## Derived Field 3: Inventory_Procured_Flag

**Purpose**: Flag indicating if an item has procurement activity (vendor sourced or pending orders).

**Business Logic**:
- If `flag_source = 'P'` (Purchased from vendor) → `'Y'`
- OR if pending release quantities exist (`Qty_Rel_PND > 0`) → `'Y'`
- OR if pending start quantities exist (`Qty_Start_PND > 0`) → `'Y'`
- Otherwise → `'N'`

**SQL Expression**:
```sql
CASE
  WHEN flag_source = 'P' THEN 'Y'
  WHEN COALESCE(Qty_Rel_PND, 0) > 0 OR COALESCE(Qty_Start_PND, 0) > 0 THEN 'Y'
  ELSE 'N'
END AS Inventory_Procured_Flag
```

**Data Type**: TEXT (fixed 1 character: 'Y' or 'N')  
**Nullability**: Non-null (always 'Y' or 'N')  
**Source Fields**: `flag_source`, `Qty_Rel_PND`, `Qty_Start_PND` (from ShopOrderData)

---

## Derived Field 4: Inventory_Released_Quantity

**Purpose**: Quantity of items in the queue for manufacturing (released to shop floor, non-pending).

**Business Logic**:
- Sum quantities from SHPORD_HDR where:
  - `stat_rec_so = 'R'` (Released status)
  - Item ID does NOT end with '#' (not pending)

**SQL Expression**:
```sql
SUM(CASE 
  WHEN stat_rec_so = 'R' AND NOT ENDSWITH(TRIM(id_item_par), '#') 
  THEN COALESCE(qty_onord, 0) 
  ELSE 0 
END) AS Inventory_Released_Quantity
```

**Data Type**: NUMBER  
**Nullability**: Non-null (0 if no released quantities)  
**Aggregation**: GROUP BY `id_item_par`, `id_loc`  
**Source Table**: SHPORD_HDR_Bronze

**Note**: Pending suffix '#' indicates SO not yet released to floor; excluded from this count.

---

## Derived Field 5: Inventory_Start_Quantity

**Purpose**: Quantity of items in manufacturing (start status, non-pending).

**Business Logic**:
- Sum quantities from SHPORD_HDR where:
  - `stat_rec_so = 'S'` (Start/In Progress status)
  - Item ID does NOT end with '#' (not pending)

**SQL Expression**:
```sql
SUM(CASE 
  WHEN stat_rec_so = 'S' AND NOT ENDSWITH(TRIM(id_item_par), '#') 
  THEN COALESCE(qty_onord, 0) 
  ELSE 0 
END) AS Inventory_Start_Quantity
```

**Data Type**: NUMBER  
**Nullability**: Non-null (0 if no start quantities)  
**Aggregation**: GROUP BY `id_item_par`, `id_loc`  
**Source Table**: SHPORD_HDR_Bronze

**Note**: Items actively being manufactured on shop floor.

---

## Derived Field 6: Inventory_Pending_Release_Quantity

**Purpose**: Quantity of items pending release to manufacturing (not yet on floor).

**Business Logic**:
- Sum quantities from SHPORD_HDR where:
  - `stat_rec_so = 'R'` (Release status)
  - Item ID ENDS WITH '#' (pending indicator)

**SQL Expression**:
```sql
SUM(CASE 
  WHEN stat_rec_so = 'R' AND ENDSWITH(TRIM(id_item_par), '#') 
  THEN COALESCE(qty_onord, 0) 
  ELSE 0 
END) AS Inventory_Pending_Release_Quantity
```

**Data Type**: NUMBER  
**Nullability**: Non-null (0 if no pending releases)  
**Aggregation**: GROUP BY `id_item_par`, `id_loc`  
**Source Table**: SHPORD_HDR_Bronze

**Note**: Pending suffix '#' on parent item ID indicates NOT yet released; these are awaiting approval/processing.

---

## Derived Field 7: Inventory_Pending_Start_Quantity

**Purpose**: Quantity of items pending start in manufacturing (approval pending).

**Business Logic**:
- Sum quantities from SHPORD_HDR where:
  - `stat_rec_so = 'S'` (Start status)
  - Item ID ENDS WITH '#' (pending indicator)

**SQL Expression**:
```sql
SUM(CASE 
  WHEN stat_rec_so = 'S' AND ENDSWITH(TRIM(id_item_par), '#') 
  THEN COALESCE(qty_onord, 0) 
  ELSE 0 
END) AS Inventory_Pending_Start_Quantity
```

**Data Type**: NUMBER  
**Nullability**: Non-null (0 if no pending starts)  
**Aggregation**: GROUP BY `id_item_par`, `id_loc`  
**Source Table**: SHPORD_HDR_Bronze

**Note**: Pending suffix '#' indicates awaiting manufacturing start approval.

---

## Derived Field 8: Inventory_Cut_Quantity

**Purpose**: Quantity actively being cut/processed (combination of all manufacturing quantities under cut policy).

**Business Logic**:
- If `flag_stk IN ('S', 'M')` (Stock or Manufacturing flag) OR `Inventory_Procured_Flag = 'Y'` → Sum of Start + Pending Start quantities
- Otherwise → 0

**SQL Expression**:
```sql
CASE
  WHEN flag_stk IN ('S', 'M') OR Inventory_Procured_Flag = 'Y'
  THEN COALESCE(Inventory_Start_Quantity, 0) + COALESCE(Inventory_Pending_Start_Quantity, 0)
  ELSE 0
END AS Inventory_Cut_Quantity
```

**Data Type**: NUMBER  
**Nullability**: Non-null (always 0 or positive)  
**Source Fields**: `flag_stk`, `Inventory_Procured_Flag`, `Inventory_Start_Quantity`, `Inventory_Pending_Start_Quantity`

**Note**: Represents quantity currently in "cut" phase (cutting stock, preparing for manufacturing). Used for work-in-progress tracking.

---

## Implementation Order

1. **Phase 1 (Foundation)**: Fields 4, 5, 6, 7 (ShopOrder aggregations via `ShopOrderData` CTE in base pipeline)
2. **Phase 2 (Flags)**: Field 3 (Procured_Flag — simple case expression)
3. **Phase 3 (Complex Joins)**: Field 1 (In-Transit — requires separate branch with aggregation)
4. **Phase 4 (Dependent Derivations)**: Field 8 (Cut_Quantity — depends on Field 3, 5, 6) & Field 2 (Primary_Location — complex 3-component join)

---

## Data Quality Validation

| Field | Expected Nullability | Data Type | Validation Rule |
|-------|---------------------|-----------|-----------------|
| In_Transit | 0% null | NUMBER | Always ≥ 0 |
| Primary_Location_Flag | 0% null | TEXT | Always 'Y' or 'N' |
| Procured_Flag | 0% null | TEXT | Always 'Y' or 'N' |
| Released_Quantity | 0% null | NUMBER | Always ≥ 0 |
| Start_Quantity | 0% null | NUMBER | Always ≥ 0 |
| Pending_Release_Quantity | 0% null | NUMBER | Always ≥ 0 |
| Pending_Start_Quantity | 0% null | NUMBER | Always ≥ 0 |
| Cut_Quantity | 0% null | NUMBER | Always ≥ 0 |

**QA Note**: All fields must be non-null post-derivation. Use `COALESCE(field, 0)` or `COALESCE(field, 'N')` in final projection to guarantee no nulls escape to consumer.

