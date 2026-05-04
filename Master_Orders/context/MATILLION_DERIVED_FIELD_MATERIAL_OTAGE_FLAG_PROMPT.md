# Matillion Prompt: Append Material_Otage_Flag to Master_Orders_Silver

## Objective
Update the existing Master_Orders_Silver Matillion pipeline to append a new derived field:
- `Material_Otage_Flag`

The field must be derived from `CP_ORDHDR_CUSTOM_COMMENTS_Bronze` comment text using the highlighted business rule:
- If comment contains `#MO`, then flag = `Y`
- Else flag = `N`

## Source Tables
- `BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze"` (or existing order-header stream already in pipeline)
- `BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_CUSTOM_COMMENTS_Bronze"`

## Required Business Logic
Implement the equivalent of this SQL logic in Matillion:
```sql
SELECT
    oh.id_ord,
    cc.comment,
    cc.flag_del,
    CASE WHEN cc.comment LIKE '%#MO%' THEN 'Y' ELSE 'N' END AS flag_MO
FROM nsa.cp_ordhdr oh
LEFT JOIN (
    SELECT *
    FROM nsa.cp_ordhdr_custom_comments
    WHERE ISNULL(flag_del, '') <> 'D'
) cc
    ON oh.id_ord = cc.id_ord;
```

Snowflake-compatible expression:
```sql
CASE
  WHEN COALESCE(c.ORD_COMMENT, '') ILIKE '%#MO%' THEN 'Y'
  ELSE 'N'
END AS Material_Otage_Flag
```

## Implementation Notes (Match Existing Master_Orders Pattern)
Use the existing `ORD_COMMENTS` CTE/join pattern in `Master_Orders_Table.sql` where comments are already deduped to latest by `ID_ORD` and soft deletes are excluded (`COALESCE(FLAG_DEL, '') <> 'D'`).

### Component Steps
1. Keep or reuse existing `ORD_COMMENTS` extraction from `CP_ORDHDR_CUSTOM_COMMENTS_Bronze`.
2. Ensure comments are filtered to exclude deleted rows:
   - `COALESCE(FLAG_DEL, '') <> 'D'`
3. Keep join from order stream to comments on `ID_ORD` (LEFT JOIN).
4. In final projection for Master_Orders_Silver, append:
```sql
CASE
  WHEN COALESCE(c.ORD_COMMENT, '') ILIKE '%#MO%' THEN 'Y'
  ELSE 'N'
END AS Material_Otage_Flag
```

## Column Requirements
- Output column name: `Material_Otage_Flag`
- Output values allowed: `Y`, `N` only
- Null-safe behavior: if no comment row exists, output must be `N`

## Validation Checklist
1. Row count stability:
   - Row count before and after adding `Material_Otage_Flag` remains unchanged.
2. Null handling:
   - No NULL values in `Material_Otage_Flag`.
3. Rule check:
   - For rows where `Order_Comment_Operations` contains `#MO`, `Material_Otage_Flag = 'Y'`.
   - Otherwise `Material_Otage_Flag = 'N'`.
4. Spot-check sample:
```sql
SELECT
  "Order_ID",
  "Order_Comment_Operations",
  "Material_Otage_Flag"
FROM SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE_SILVER
WHERE "Order_Comment_Operations" ILIKE '%#MO%'
LIMIT 50;
```

## Deliverables
1. Updated Matillion graph/component expressions with `Material_Otage_Flag` appended.
2. Updated final SELECT projection in Master_Orders_Silver output.
3. QA evidence:
   - sample rows showing both `Y` and `N`
   - confirmation of unchanged row count
   - confirmation of no nulls in `Material_Otage_Flag`
