# Matillion Prompt - STAT_REC_SO_DISPLAY Derived Flag

Use this prompt in Matillion AI / Copilot to generate the SQL for a derived field named `STAT_REC_SO_DISPLAY`.

---

Create a Snowflake SQL transformation that derives `STAT_REC_SO_DISPLAY` for shop orders.

## Objective
Build a derived status flag that mirrors the legacy OOwP business logic for shop order display status.

The derived flag must be created after joining shop order header and operation status data, and then used to populate quantity buckets for released, cut, and pre-sew-complete work.

## Business Context
This logic supports the open orders reporting process used by operations, customer service, and planning.

The report is not only a backlog list. It is used to:
- communicate production status to customer service
- determine daily shipping capacity and prioritization
- identify delays caused by material or production constraints
- support operational planning decisions across stock and made-to-order demand

The business context has shifted from mostly made-to-order production to a mix closer to stock plus made-to-order, so more granular production visibility is now needed.

This derived field is a stepping stone toward a broader control-tower style planning model discussed in the Tuesday AI meeting, where future reporting may incorporate predictive signals, market inputs, and operational constraints.

## Meeting Requirements
Implement the following business requirements in the SQL generated from this prompt:
1. Create a derived `STAT_REC_SO_DISPLAY` field using both shop order header status and operation status.
2. Build quantity buckets using the derived display flag.
3. Validate that the bucket totals reconcile to total on-order quantity.
4. Preserve enough business context in comments so this logic can be reused in future control-tower reporting discussions.

## Quantity Bucket Requirements
After deriving `STAT_REC_SO_DISPLAY`, create these quantity buckets:
- `Qty_Released`: quantity where `STAT_REC_SO_DISPLAY = 'R'`
- `Qty_Cut`: quantity where `STAT_REC_SO_DISPLAY = 'S'`
- `Qty_Presew`: quantity where `STAT_REC_SO_DISPLAY IN ('W', 'D')`

Include a validation expression or validation query that confirms:

```sql
COALESCE(Qty_Released, 0) + COALESCE(Qty_Cut, 0) + COALESCE(Qty_Presew, 0) = COALESCE(QTY_ONORD, 0)
```

If the output is aggregated, validate at the same grain as the quantity output.

## Shop Order Display Status Meaning
Use these business meanings when deriving and documenting the field:
- `R` = released but not yet cut; the shop order is released, but no labor operation has started yet
- `S` = started or cut; cutting is complete and the order is ready for the next stage
- `W` = pre-sew complete; this is an artificial derived status and is not stored directly in TCM/TPCO
- `D` = done in assembly; all production is complete and the product is ready for shipping pickup

## Why W Exists
The `W` status exists because pre-sew operations are not truly sequential in practice.

Pre-sew can contain several independent steps such as pockets, embroidery, or trim. Those steps may be completed in any order depending on workload. Because TCM routing is linear, relying on ordinary operation order can incorrectly imply the job is ready for assembly too early.

To solve this, the process inserts operation `3999` as a pre-sew-complete checkpoint. `W` should be assigned when the pre-sew-complete operation is closed while the final operation is not yet ready.

For items with no meaningful pre-sew stage, the business still wants the logic to treat the shop order as pre-sew complete when the order is started and the final operation is ready.

## Business Status Progressions
Use these progressions as reference comments in the generated SQL:

### Shop Order Status Progression
- `U` = unallocated or unreleased
- `A` = allocated
- `R` = released
- `S` = started
- `E` = ended, awaiting approval in the old process
- `C` = complete

Artificial sub-statuses within started work:
- `W` = pre-sew complete
- `D` = done in assembly

### Operation Status Progression
- `P` = planned
- `R` = ready
- `A` = active
- `C` = complete

## Primary Source Tables
Use the following source tables and aliases:
- `BRONZE_DATA.TCM_BRONZE."SHPORD_HDR_Bronze"` as `sh`
- `BRONZE_DATA.TCM_BRONZE."SHPORD_OPER_Bronze"` as `so`

If the physical source in Matillion is named `SHPORD_HSR` in your environment, substitute that source for `SHPORD_HDR_Bronze`, but keep the header alias as `sh`.

## Important Rule Dependency
The original legacy logic also references item planner (`ilPAR.ID_PLANNER`) to decide whether status should become `D`.

Exact parity with the legacy CASE expression is not possible from only `SHPORD_HDR` and `SHPORD_OPER` unless planner is also available from an item/location source.

Handle this as follows:
1. If planner is available in the pipeline from an additional joined source, use the exact logic.
2. If planner is not available, implement the reduced two-table version and clearly comment that the `D` rule is approximated.

## Business Logic To Replicate
The legacy CASE expression is:

```sql
CASE
    WHEN sh.STAT_REC_SO = 'S' AND sh.DATE_START_OPER_1ST IS NULL THEN 'R'
    WHEN so3999.STAT_REC_OPER = 'C' AND so9999.STAT_REC_OPER <> 'R' THEN 'W'
    WHEN so3999.STAT_REC_OPER IS NULL AND sh.STAT_REC_SO = 'S' AND so9999.STAT_REC_OPER <> 'R' THEN 'W'
    WHEN so9999.STAT_REC_OPER = 'R' AND ilPAR.ID_PLANNER NOT LIKE 'KT' THEN 'D'
    ELSE sh.STAT_REC_SO
END AS STAT_REC_SO_DISPLAY
```

## Required Join Logic
1. Start from `sh` at shop order header grain.
- Grain should be one row per `ID_LOC`, `ID_SO`, `SUFX_SO`.
- Bring forward the quantity field needed for reconciliation, such as `QTY_ONORD` if available in the selected source model.

2. Join operation 3999 separately.
- Create a CTE or inline derived table filtered to `ID_OPER = '3999'`.
- Alias it as `so3999`.
- Join on trimmed `ID_LOC`, `ID_SO`, and `SUFX_SO`.
- Use this operation as the pre-sew-complete checkpoint.

3. Join operation 9999 separately.
- Create a CTE or inline derived table filtered to `ID_OPER = '9999'`.
- Alias it as `so9999`.
- Join on trimmed `ID_LOC`, `ID_SO`, and `SUFX_SO`.
- Use this operation to determine whether final production is ready or complete.

4. If planner is available from another source:
- Join that planner source at the item/location level.
- Use planner in the `D` rule exactly as `planner NOT LIKE 'KT'`.

5. Derive the display flag after the joins.
- Do not attempt to derive `STAT_REC_SO_DISPLAY` before the operation joins are in place.
- Derive the quantity buckets only after `STAT_REC_SO_DISPLAY` is available.

## Data Quality Rules
- TRIM all ID-like join columns at the CTE level before downstream joins.
- At minimum trim:
  - `ID_LOC`
  - `ID_SO`
  - `SUFX_SO`
  - `ID_ITEM_PAR` if brought forward
- Use null-safe comparisons where appropriate.
- Preserve header grain and do not duplicate shop orders.

## Preferred CTE Structure
Use this structure:
- `shop_order_hdr_base`
- `oper_3999`
- `oper_9999`
- `shop_order_status_derived`
- `quantity_buckets`
- `bucket_validation`
- `final_select`

## Output Requirements
Return these columns at minimum:
- `ID_LOC`
- `ID_SO`
- `SUFX_SO`
- `QTY_ONORD`
- `STAT_REC_SO`
- `DATE_START_OPER_1ST`
- `STAT_REC_OPER_3999`
- `STAT_REC_OPER_9999`
- `STAT_REC_SO_DISPLAY`
- `Qty_Released`
- `Qty_Cut`
- `Qty_Presew`
- `Qty_Bucket_Total`
- `Qty_Bucket_Recon_Flag`

Use final output aliases in Title_Case with underscore separation where business-facing aliases are needed.

## Exact Version
If planner is available, derive the field with this logic:

```sql
CASE
    WHEN sh.STAT_REC_SO = 'S' AND sh.DATE_START_OPER_1ST IS NULL THEN 'R'
    WHEN so3999.STAT_REC_OPER = 'C' AND COALESCE(so9999.STAT_REC_OPER, '') <> 'R' THEN 'W'
    WHEN so3999.STAT_REC_OPER IS NULL AND sh.STAT_REC_SO = 'S' AND COALESCE(so9999.STAT_REC_OPER, '') <> 'R' THEN 'W'
    WHEN so9999.STAT_REC_OPER = 'R' AND planner NOT LIKE 'KT' THEN 'D'
    ELSE sh.STAT_REC_SO
END AS STAT_REC_SO_DISPLAY
```

## Reduced Two-Table Version
If only header and operation tables are allowed, derive the field with this fallback and add a SQL comment that the planner condition is unavailable:

```sql
CASE
    WHEN sh.STAT_REC_SO = 'S' AND sh.DATE_START_OPER_1ST IS NULL THEN 'R'
    WHEN so3999.STAT_REC_OPER = 'C' AND COALESCE(so9999.STAT_REC_OPER, '') <> 'R' THEN 'W'
    WHEN so3999.STAT_REC_OPER IS NULL AND sh.STAT_REC_SO = 'S' AND COALESCE(so9999.STAT_REC_OPER, '') <> 'R' THEN 'W'
    WHEN so9999.STAT_REC_OPER = 'R' THEN 'D'
    ELSE sh.STAT_REC_SO
END AS STAT_REC_SO_DISPLAY
```

## Technical Requirements
- Snowflake SQL only.
- Use clear comments for each CTE.
- Do not use `SELECT *` in the final select.
- Ensure one output row per shop order business key.
- Include comments explaining that `W` is a derived business status created to handle non-sequential pre-sew processing.
- Include a reconciliation check that confirms bucket totals equal on-order quantity.

## Delivery
Return:
1. A full SELECT statement that derives `STAT_REC_SO_DISPLAY`
2. Quantity bucket logic for `Qty_Released`, `Qty_Cut`, and `Qty_Presew`
3. A reconciliation field or validation query showing whether quantity buckets sum to on-order quantity
4. Optionally, a `CREATE OR REPLACE VIEW` wrapper
5. A short note stating whether the exact or reduced logic was used