-- CREATE OR REPLACE DYNAMIC TABLE SILVER_DATA.TCM_SILVER.MASTER_ORDERS_TABLE
--     TARGET_LAG   = 'DOWNSTREAM'
--     REFRESH_MODE = AUTO
--     INITIALIZE   = ON_CREATE
--     WAREHOUSE    = ELT_DEFAULT
-- AS
/* ============================================================
   ORD_HDR — Order header fields (one row per order)
   Sources: CP_ORDHDR_Bronze (active) ∪ CP_ORDHDR_PERM_Bronze (closed)
   ============================================================ */
WITH ORD_HDR AS (
    SELECT
        TRIM(h.ID_ORD)              AS ID_ORD,
        'ACTIVE'                    AS HDR_SOURCE_TABLE,

        -- Customer
        TRIM(h.ID_CUST_SOLDTO)      AS ID_CUST_SOLDTO,
        h.SEQ_SHIPTO,
        TRIM(h.ID_CUST_BILLTO)      AS ID_CUST_BILLTO,
        TRIM(h.ID_PO_CUST)          AS ID_PO_CUST,

        -- Accounting
        TRIM(h.ACCT_ID_AR)          AS ACCT_ID_AR,
        TRIM(h.ACCT_ID_DEP)         AS ACCT_ID_DEP,
        TRIM(h.ACCT_ID_TAX)         AS ACCT_ID_TAX,
        TRIM(h.ACCT_ID_FRT)         AS ACCT_ID_FRT,
        TRIM(h.ACCT_ID_CHRG_MISC)   AS ACCT_ID_CHRG_MISC,
        TRIM(h.ACCT_ID_FEE_RESTOCK) AS ACCT_ID_FEE_RESTOCK,
        h.ACCT_DEPT_TAX             AS ACCT_DIV_TAX,
        h.ACCT_DEPT_FRT,
        h.ACCT_DEPT_CHRG_MISC,
        h.ACCT_DEPT_FEE_RESTOCK,

        -- Order Classification
        h.TYPE_ORD_CP,
        h.CODE_STAT_ORD,
        h.CODE_SRC_EDI,
        h.ABBRV_CONSIG,

        -- Sales
        TRIM(h.ID_SLSREP_1)         AS ID_SLSREP_1,

        -- Flags
        h.FLAG_ASN_EDI,
        h.FLAG_INVC_EDI,
        h.FLAG_PAID_BY_CC,
        h.FLAG_810,

        -- Dates
        h.DATE_ORD,
        h.DATE_ADD,

        -- Terms
        h.CODE_TRMS_CP,
        h.DESCR_TRMS,
        h.PCT_DISC_TRMS,
        h.PCT_DISC_ORD_1,

        -- Financials (header-level totals)
        h.AMT_ORD_TOTAL,
        h.COST_TOTAL,
        h.AMT_FRT,

        -- Reference
        TRIM(h.ID_ORD_WEB)          AS ID_ORD_WEB,
        TRIM(h.ID_DOC_APPLYTO)      AS ID_DOC_APPLYTO,
        TRIM(h.ID_REL)              AS ID_REL,
        TRIM(h.ID_REV)              AS ID_REV,

        -- User
        TRIM(h.ID_USER_ADD)         AS ID_USER_ADD,

        -- Miscellaneous
        h.ATTACH_COMMENT,
        h.RATE_EXCHG_CRNCY,
        h.AMT_DISC

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_Bronze" h

    UNION ALL

    SELECT
        TRIM(p.ID_ORD)              AS ID_ORD,
        'PERM'                      AS HDR_SOURCE_TABLE,

        TRIM(p.ID_CUST_SOLDTO)      AS ID_CUST_SOLDTO,
        p.SEQ_SHIPTO,
        TRIM(p.ID_CUST_BILLTO)      AS ID_CUST_BILLTO,
        TRIM(p.ID_PO_CUST)          AS ID_PO_CUST,

        -- Accounting
        TRIM(p.ACCT_ID_AR)          AS ACCT_ID_AR,
        TRIM(p.ACCT_ID_DEP)         AS ACCT_ID_DEP,
        TRIM(p.ACCT_ID_TAX)         AS ACCT_ID_TAX,
        TRIM(p.ACCT_ID_FRT)         AS ACCT_ID_FRT,
        TRIM(p.ACCT_ID_CHRG_MISC)   AS ACCT_ID_CHRG_MISC,
        TRIM(p.ACCT_ID_FEE_RESTOCK) AS ACCT_ID_FEE_RESTOCK,
        p.ACCT_DEPT_TAX             AS ACCT_DIV_TAX,
        p.ACCT_DEPT_FRT,
        p.ACCT_DEPT_CHRG_MISC,
        p.ACCT_DEPT_FEE_RESTOCK,

        p.TYPE_ORD_CP,
        p.CODE_STAT_ORD,
        p.CODE_SRC_EDI,

        p.ABBRV_CONSIG,

        TRIM(p.ID_SLSREP_1)         AS ID_SLSREP_1,

        -- Flags
        p.FLAG_ASN_EDI,
        p.FLAG_INVC_EDI,
        p.FLAG_PAID_BY_CC,
        p.FLAG_810,

        p.DATE_ORD,
        p.DATE_ADD,

        p.CODE_TRMS_CP,
        p.DESCR_TRMS,
        p.PCT_DISC_TRMS,
        p.PCT_DISC_ORD_1,

        p.AMT_ORD_TOTAL,
        p.COST_TOTAL,
        p.AMT_FRT,

        TRIM(p.ID_ORD_WEB)          AS ID_ORD_WEB,
        TRIM(p.ID_DOC_APPLYTO)      AS ID_DOC_APPLYTO,
        TRIM(p.ID_REL)              AS ID_REL,
        TRIM(p.ID_REV)              AS ID_REV,

        TRIM(p.ID_USER_ADD)         AS ID_USER_ADD,

        p.ATTACH_COMMENT,
        p.RATE_EXCHG_CRNCY,
        p.AMT_DISC

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_PERM_Bronze" p
),

/* ============================================================
   ORD_LIN — Order line detail (one row per order + line)
   Sources: CP_ORDLIN_Bronze (active) ∪ CP_ORDLIN_PERM_Bronze (closed)
   ============================================================ */
ORD_LIN AS (
    SELECT
        TRIM(l.ID_ORD)              AS ID_ORD,
        l.SEQ_LINE_ORD,
        l.SEQ_REV_QUOTE,
        'ACTIVE'                    AS LIN_SOURCE_TABLE,

        -- Item
        TRIM(l.ID_ITEM)             AS ID_ITEM,
        TRIM(l.ID_ITEM_CUST)        AS ID_ITEM_CUST,
        TRIM(l.ID_LOC)              AS ID_LOC,
        l.CODE_USER_1_IM,
        l.CODE_USER_2_IM,

        -- Pricing (stored as varchar in source — kept as-is for now)
        l.PRICE_LIST_VP,
        l.COST_UNIT_VP,
        l.PRICE_NET,
        l.RATIO_PRICE_SELL,
        l.PRICE_SELL_VP,

        -- Dates (line-level)
        l.DATE_RQST,
        l.DATE_PROM,
        l.DATE_PICK_LAST,
        l.DATE_ACKN_LAST,
        l.DATE_CHG,
        l.TIME_CHG,

        -- Unit of Measure
        l.CODE_UM_ORD,
        l.CODE_UM_PRICE,

        -- Flags
        l.FLAG_PICK,
        l.FLAG_BO,
        l.FLAG_ACKN,

        -- Shop Order Linkage (FK to MASTER_SHOPORDER_TABLE)
        TRIM(l.ID_LOC_SO)           AS ID_LOC_SO,
        TRIM(l.ID_SO)               AS ID_SO,
        l.SUFX_SO,

        -- Backorder
        l.VER_BO,
        l.ATTACH_COMMENT,

        -- Quote
        TRIM(l.ID_QUOTE)            AS ID_QUOTE,

        -- User
        TRIM(l.ID_USER_CHG)         AS ID_USER_CHG,

        -- Custom Fields
        l.CSTM_DATE_1,
        l.CSTM_DATE_2,
        l.CSTM_DATE_3,
        l.CSTM_FLAG_3,
        l.FLAG_OPTION_ATPIC,
        TRIM(l.ID_LINE_PO)          AS ID_LINE_PO,
        TRIM(l.ID_LINE_PO_EDI)      AS ID_LINE_PO_EDI

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze" l

    UNION ALL

    SELECT
        TRIM(p.ID_ORD)              AS ID_ORD,
        p.SEQ_LINE_ORD,
        p.SEQ_REV_QUOTE,
        'PERM'                      AS LIN_SOURCE_TABLE,

        TRIM(p.ID_ITEM)             AS ID_ITEM,
        TRIM(p.ID_ITEM_CUST)        AS ID_ITEM_CUST,
        TRIM(p.ID_LOC)              AS ID_LOC,
        p.CODE_USER_1_IM,
        p.CODE_USER_2_IM,

        p.PRICE_LIST_VP,
        p.COST_UNIT_VP,
        p.PRICE_NET,
        p.RATIO_PRICE_SELL,
        p.PRICE_SELL_VP,

        p.DATE_RQST,
        p.DATE_PROM,
        p.DATE_PICK_LAST,
        p.DATE_ACKN_LAST,
        p.DATE_CHG,
        p.TIME_CHG,

        p.CODE_UM_ORD,
        p.CODE_UM_PRICE,

        p.FLAG_PICK,
        p.FLAG_BO,
        p.FLAG_ACKN,

        TRIM(p.ID_LOC_SO)           AS ID_LOC_SO,
        TRIM(p.ID_SO)               AS ID_SO,
        p.SUFX_SO,

        p.VER_BO,
        p.ATTACH_COMMENT,

        TRIM(p.ID_QUOTE)            AS ID_QUOTE,

        TRIM(p.ID_USER_CHG)         AS ID_USER_CHG,

        -- Custom Fields
        p.CSTM_DATE_1,
        p.CSTM_DATE_2,
        p.CSTM_DATE_3,
        p.CSTM_FLAG_3,
        p.FLAG_OPTION_ATPIC,
        TRIM(p.ID_LINE_PO)          AS ID_LINE_PO,
        TRIM(p.ID_LINE_PO_EDI)      AS ID_LINE_PO_EDI

    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_PERM_Bronze" p
),

/* ============================================================
   ORD_COMMENTS — Custom comments / ship-date overrides
   Deduped to latest per ID_ORD; soft-deletes excluded
   Source: BRONZE_DATA.TCM_BRONZE.CP_ORDHDR_CUSTOM_COMMENTS_Bronze
   ============================================================ */

ORD_COMMENTS AS (
    SELECT
        c.ID_ORD,
        c.DATE_EST_SHIP             AS ORD_COMMENT_DATE_EST_SHIP,
        c.DATE_OLD_SHIP             AS ORD_COMMENT_DATE_OLD_SHIP,
        c.COMMENT                   AS ORD_COMMENT,
        c.DATE_ADD                  AS ORD_COMMENT_DATE_ADD,
        c.ID_USER_ADD               AS ORD_COMMENT_ID_USER_ADD,
        c.DATE_CHG                  AS ORD_COMMENT_DATE_CHG,
        c.ID_USER_CHG               AS ORD_COMMENT_ID_USER_CHG,
        c."rowid"                   AS ORD_COMMENT_ROWID,
        c.FLAG_DEL                  AS ORD_COMMENT_FLAG_DEL,
        c.LATE_CODE                 AS ORD_COMMENT_LATE_CODE,    --! LATE_CODE values are defined as follows (per business)? [ NULL->79587,  1->307, 2->272, 9->48, 6->43, 3->10, 4->6, 5->4 ]   LATE_ORDER_CODE table in TCM to get the description.
        c."rowversion"              AS ORD_COMMENT_ROWVERSION
    FROM (
        SELECT
            TRIM(ID_ORD)            AS ID_ORD,
            DATE_EST_SHIP,
            DATE_OLD_SHIP,
            COMMENT,
            DATE_ADD,
            TRIM(ID_USER_ADD)       AS ID_USER_ADD,
            DATE_CHG,
            TRIM(ID_USER_CHG)       AS ID_USER_CHG,
            "rowid",
            FLAG_DEL,
            LATE_CODE,
            "rowversion",
            ROW_NUMBER() OVER (
                PARTITION BY ID_ORD
                ORDER BY COALESCE(DATE_CHG, DATE_ADD) DESC NULLS LAST, "rowid" DESC NULLS LAST, "rowversion" DESC NULLS LAST
            ) AS RN
        FROM BRONZE_DATA.TCM_BRONZE."CP_ORDHDR_CUSTOM_COMMENTS_Bronze"
        WHERE COALESCE(FLAG_DEL, '') <> 'D'
    ) c
    WHERE c.RN = 1
),

/* ============================================================
   LINE_COMMENTS — Line-level comments from CP_COMMENT
   Deduped to latest per order-line (ID_ORD + SEQ_LINE_ORD)
   Source: BRONZE_DATA.TCM_BRONZE.CP_COMMENT_Bronze
   ============================================================ */
LINE_COMMENTS AS (
    SELECT
        lc.ID_ORD,
        lc.ATTACH_COMMENT_FILE      AS LINE_COMMENT_ATTACH_COMMENT_FILE,
        lc.ID_SHIP                  AS LINE_COMMENT_ID_SHIP,
        lc.SEQ_LINE_ORD,
        lc.CODE_COMMENT             AS LINE_COMMENT_CODE_RAW,
        lc.SEQ_COMMENT              AS LINE_COMMENT_SEQ_COMMENT,
        lc.ID_INVC                  AS LINE_COMMENT_ID_INVC,
        lc.ID_USER_ADD              AS LINE_COMMENT_ID_USER_ADD,
        lc.DATE_ADD                 AS LINE_COMMENT_DATE_ADD,
        lc.TIME_ADD                 AS LINE_COMMENT_TIME_ADD,
        lc.ID_USER_CHG              AS LINE_COMMENT_ID_USER_CHG,
        lc.DATE_CHG                 AS LINE_COMMENT_DATE_CHG,
        lc.TIME_CHG                 AS LINE_COMMENT_TIME_CHG,
        lc.NOTE                     AS LINE_COMMENT_NOTE_RAW,
        lc.FLAG_PRNT_PO             AS LINE_COMMENT_FLAG_PRNT_PO,
        lc.NOTE                     AS LINE_COMMENT_NOTE,
        lc.CODE_COMMENT             AS LINE_COMMENT_CODE,
        lc.CODE_QLFR                AS LINE_COMMENT_QLFR,
        lc.REF                      AS LINE_COMMENT_REF
    FROM (
        SELECT
            TRIM(ID_ORD)            AS ID_ORD,
            ATTACH_COMMENT_FILE,
            TRIM(ID_SHIP)           AS ID_SHIP,
            SEQ_LINE_ORD,
            CODE_COMMENT,
            SEQ_COMMENT,
            TRIM(ID_INVC)           AS ID_INVC,
            TRIM(ID_USER_ADD)       AS ID_USER_ADD,
            DATE_ADD,
            TIME_ADD,
            TRIM(ID_USER_CHG)       AS ID_USER_CHG,
            DATE_CHG,
            TIME_CHG,
            NOTE,
            FLAG_PRNT_PO,
            CODE_QLFR,
            REF,
            ROW_NUMBER() OVER (
                PARTITION BY ID_ORD, SEQ_LINE_ORD
                ORDER BY COALESCE(DATE_CHG, DATE_ADD) DESC NULLS LAST, "rowid" DESC NULLS LAST, "rowversion" DESC NULLS LAST
            ) AS RN
        FROM BRONZE_DATA.TCM_BRONZE."CP_COMMENT_Bronze"
    ) lc
    WHERE lc.RN = 1
)

/* ============================================================
   FINAL SELECT — Order-line grain master table
   Header fields denormalized onto every line
   All columns from ORD_HDR, ORD_LIN, ORD_COMMENTS, LINE_COMMENTS
   VP pricing decoded to numeric + open-value calculations
   ============================================================ */
SELECT

    -- ── Order Key ─────────────────────────────────────────────────────────────
    l.ID_ORD                                                                AS Order_ID,
    l.SEQ_LINE_ORD                                                          AS Order_Line_Sequence_Num,
    l.SEQ_REV_QUOTE                                                         AS Order_Quote_Revision_Sequence,

    -- ── Customer ──────────────────────────────────────────────────────────────
    h.ID_CUST_SOLDTO                                                        AS Customer_ID_Sold_To,
    h.SEQ_SHIPTO                                                            AS Customer_End_User_Ship_To_Sequence_Num,
    CONCAT(h.ID_CUST_SOLDTO, '-', LPAD(CAST(h.SEQ_SHIPTO AS VARCHAR), 4, '0'))
                                                                            AS Customer_ID_Ship_To,
    h.ID_CUST_BILLTO                                                        AS Customer_ID_Bill_To,
    h.ID_PO_CUST                                                            AS Customer_Purchase_Order_ID,

    -- ── Order Classification ───────────────────────────────────────────────────
    h.TYPE_ORD_CP                                                           AS Order_Type,
    h.CODE_STAT_ORD                                                         AS Order_Status_Code,
    h.CODE_SRC_EDI                                                          AS EDI_Source_Code,
    h.ABBRV_CONSIG                                                          AS Consignment_Abbreviation,

    -- ── Sales Rep ─────────────────────────────────────────────────────────────
    h.ID_SLSREP_1                                                           AS Employee_ID_TCM_Sales_Rep,

    -- ── Item (line-level) ─────────────────────────────────────────────────────
    l.ID_ITEM                                                               AS Item_ID_Child_SKU,
    l.ID_ITEM_CUST                                                          AS Item_ID_Customer_SKU,
    l.ID_LOC                                                                AS Order_Assigned_Location_ID,
    l.CODE_USER_1_IM                                                        AS Order_Code_User_1,
    l.CODE_USER_2_IM                                                        AS Order_Code_User_2,

    -- ── Pricing (raw VP varchar — retained for audit) ─────────────────────────
    l.PRICE_NET                                                             AS Net_Price_At_Order,
    l.RATIO_PRICE_SELL                                                      AS Order_Price_Sold_Ratio,
    l.PRICE_SELL_VP                                                         AS Sold_Price_At_Order_VP,

    -- ── Dates (header-level) ──────────────────────────────────────────────────
    h.DATE_ORD                                                              AS Date_Ordered,
    h.DATE_ADD                                                              AS Date_Order_Added,

    -- ── Dates (line-level) ────────────────────────────────────────────────────
    l.DATE_RQST                                                             AS Date_Order_Requested,
    l.DATE_PROM                                                             AS Date_Order_Promised,
    l.DATE_PICK_LAST                                                        AS Date_Order_Last_Picked,
    l.DATE_ACKN_LAST                                                        AS Date_Order_Last_Acknowledged,
    l.DATE_CHG                                                              AS Date_Order_Changed,
    l.TIME_CHG                                                              AS Date_Order_Changed_Time,

    -- ── Terms / Discounts ─────────────────────────────────────────────────────
    h.CODE_TRMS_CP                                                          AS Terms_At_Order,
    h.DESCR_TRMS                                                            AS Terms_At_Order_Description,
    h.PCT_DISC_TRMS                                                         AS Order_Discount_Percent_Terms,
    h.PCT_DISC_ORD_1                                                        AS Order_Discount_Percent_1,

    -- ── Financials (header-level) ──────────────────────────────────────────────
    h.AMT_ORD_TOTAL                                                         AS Order_Total_Amount,
    h.COST_TOTAL                                                            AS Total_Cost_At_Order,
    h.AMT_FRT                                                               AS Freight_Amount,
    h.AMT_DISC                                                              AS Discount_Amount,
    h.RATE_EXCHG_CRNCY                                                      AS Currency_Exchange_Rate,

    -- ── Accounting ────────────────────────────────────────────────────────────
    h.ACCT_ID_AR                                                            AS Accounting_Accounts_Receivable_ID,
    h.ACCT_ID_DEP                                                           AS Accounting_Department_ID,
    h.ACCT_ID_TAX                                                           AS Accounting_Tax_ID,
    h.ACCT_ID_FRT                                                           AS Accounting_Freight_ID,
    h.ACCT_ID_CHRG_MISC                                                     AS Accounting_Miscellaneous_Charge_ID,
    h.ACCT_ID_FEE_RESTOCK                                                   AS Accounting_Restock_Fee_ID,
    h.ACCT_DIV_TAX                                                          AS Accounting_Tax_DIV,
    h.ACCT_DEPT_FRT                                                         AS Accounting_Freight_Department,
    h.ACCT_DEPT_CHRG_MISC                                                   AS Accounting_Miscellaneous_Charge_Department,
    h.ACCT_DEPT_FEE_RESTOCK                                                 AS Accounting_Restocking_Fee_Department,

    -- ── Unit of Measure ───────────────────────────────────────────────────────
    l.CODE_UM_ORD                                                           AS Order_Unit_Of_Measure_Code,
    l.CODE_UM_PRICE                                                         AS Order_Unit_Of_Measure_Price,

    -- ── Flags (header-level) ──────────────────────────────────────────────────
    h.FLAG_ASN_EDI                                                          AS EDI_Advance_Shipment_Flag,
    h.FLAG_INVC_EDI                                                         AS EDI_Invoice_Capable_Flag,
    h.FLAG_PAID_BY_CC                                                       AS Planned_Credit_Card_Payment_Flag,
    h.FLAG_810                                                              AS EDI_Invoiced_Flag,

    -- ── Flags (line-level) ────────────────────────────────────────────────────
    l.FLAG_PICK                                                             AS Order_Picked_Flag,
    l.FLAG_BO                                                               AS Order_Backorder_Flag,
    l.FLAG_ACKN                                                             AS Order_Acknowledged_Flag,
    l.ATTACH_COMMENT                                                        AS Order_Line_Comment_Attached_Flag,

    -- ── Shop Order Linkage (FK → MASTER_SHOPORDER_TABLE) ──────────────────────
    l.ID_LOC_SO                                                             AS Shop_Order_Location_ID,
    l.ID_SO                                                                 AS Shop_Order_ID,
    l.SUFX_SO                                                               AS Shop_Order_ID_Suffix,

    -- ── Backorder ─────────────────────────────────────────────────────────────
    l.VER_BO                                                                AS Backorder_Version,

    -- ── Reference ─────────────────────────────────────────────────────────────
    h.ID_ORD_WEB                                                            AS Order_ID_Web,
    h.ID_DOC_APPLYTO                                                        AS Document_Applied_To_ID,
    h.ID_REL                                                                AS Order_Release_ID,
    h.ID_REV                                                                AS Order_Revision_ID,
    l.ID_QUOTE                                                              AS Customer_Quote_ID,

    -- ── User ──────────────────────────────────────────────────────────────────
    h.ID_USER_ADD                                                           AS Employee_ID_User_Add_Order,
    l.ID_USER_CHG                                                           AS Employee_ID_User_Change_Order,

    -- ── Custom Fields (line-level) ────────────────────────────────────────────
    l.CSTM_DATE_1                                                           AS Custom_Date_1,
    l.CSTM_DATE_2                                                           AS Custom_Date_2,
    l.CSTM_DATE_3                                                           AS Custom_Date_3,
    l.CSTM_FLAG_3                                                           AS Custom_Flag_3,
    l.FLAG_OPTION_ATPIC                                                     AS Option_ATPIC_Flag,
    l.ID_LINE_PO                                                            AS Order_Line_PO_ID,
    l.ID_LINE_PO_EDI                                                        AS Order_Line_PO_EDI_ID,

    -- ── Comments / Ship Dates (ORD_COMMENTS) ──────────────────────────────────
    c.ORD_COMMENT_DATE_EST_SHIP                                             AS Order_Comment_Operations_Date_Shipment_Estimate,
    c.ORD_COMMENT_DATE_OLD_SHIP                                             AS Order_Comment_Operations_Date_Shipment_Estimate_Old,
    c.ORD_COMMENT                                                           AS Order_Comment_Operations,
    c.ORD_COMMENT_DATE_ADD                                                  AS Date_Order_Comment_Operations_Added,
    c.ORD_COMMENT_ID_USER_ADD                                               AS Employee_ID_User_Add_Order_Comment_Operations,
    c.ORD_COMMENT_DATE_CHG                                                  AS Date_Order_Comment_Operations_Changed,
    c.ORD_COMMENT_ID_USER_CHG                                               AS Employee_ID_User_Change_Order_Comment_Operations,
    c.ORD_COMMENT_FLAG_DEL                                                  AS Order_Comment_Operations_Deleted_Flag,
    c.ORD_COMMENT_LATE_CODE                                                 AS Order_Comment_Operations_Late_Code,

    -- ── Line Comments (LINE_COMMENTS) ─────────────────────────────────────────
    lc.LINE_COMMENT_NOTE                                                    AS Order_Comment_CX_Note,
    lc.LINE_COMMENT_CODE                                                    AS Order_Comment_CX_Code,
    lc.LINE_COMMENT_SEQ_COMMENT                                             AS Order_Comment_CX_Sequence,
    lc.LINE_COMMENT_ATTACH_COMMENT_FILE                                     AS Order_Comment_CX_Attached_File,
    lc.LINE_COMMENT_FLAG_PRNT_PO                                            AS Order_Comment_CX_Print_Purchase_Order_Flag,
    lc.LINE_COMMENT_QLFR                                                    AS Order_Line_Comment_CX_Qualifier,
    lc.LINE_COMMENT_REF                                                     AS Order_Line_Comment_CX_Reference,
    lc.LINE_COMMENT_ID_SHIP                                                 AS Order_Comment_CX_Shipment_ID,
    lc.LINE_COMMENT_ID_INVC                                                 AS Order_Comment_CX_Invoice_ID,
    lc.LINE_COMMENT_ID_USER_ADD                                             AS Employee_ID_User_Add_Order_Comment_CX,
    lc.LINE_COMMENT_DATE_ADD                                                AS Date_Order_Comment_CX_Added,
    lc.LINE_COMMENT_TIME_ADD                                                AS Date_Order_Comment_CX_Added_Time,
    lc.LINE_COMMENT_ID_USER_CHG                                             AS Employee_ID_User_Change_Order_Comment_CX,
    lc.LINE_COMMENT_DATE_CHG                                                AS Date_Order_Comment_CX_Changed,
    lc.LINE_COMMENT_TIME_CHG                                                AS Date_Order_Comment_CX_Changed_Time

FROM ORD_LIN l
INNER JOIN ORD_HDR h
    ON l.ID_ORD = h.ID_ORD
LEFT JOIN ORD_COMMENTS c
    ON l.ID_ORD = c.ID_ORD
LEFT JOIN LINE_COMMENTS lc
    ON l.ID_ORD = lc.ID_ORD
    AND l.SEQ_LINE_ORD = lc.SEQ_LINE_ORD

/* ============================================================
   Dedupe Guard — enforce one row per business key
   Business key: (ID_ITEM, ID_ORD, SEQ_LINE_ORD)
   Priority: ACTIVE header over PERM, then date recency
   ============================================================ */
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY l.ID_ITEM, l.ID_ORD, l.SEQ_LINE_ORD
    ORDER BY
        CASE WHEN h.HDR_SOURCE_TABLE = 'ACTIVE' THEN 0 ELSE 1 END,
        COALESCE(l.DATE_PROM, h.DATE_ORD, TO_DATE('1900-01-01')) DESC,
        COALESCE(l.ID_LOC, '') ASC,
        COALESCE(l.ID_SO, '') ASC,
        COALESCE(l.SUFX_SO, 0) ASC
) = 1


/* Overlap business keys between active and perm tables (should be zero if source data is clean)
WITH lin_src AS (
    SELECT ID_ITEM, ID_ORD, SEQ_LINE_ORD, ID_LOC, 'ACTIVE' AS src
    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_Bronze"
    UNION ALL
    SELECT ID_ITEM, ID_ORD, SEQ_LINE_ORD, ID_LOC, 'PERM' AS src
    FROM BRONZE_DATA.TCM_BRONZE."CP_ORDLIN_PERM_Bronze"
),
k AS (
    SELECT
        ID_ITEM, ID_ORD, SEQ_LINE_ORD,
        COUNT(*) AS rows_per_key,
        COUNT_IF(src = 'ACTIVE') AS active_rows,
        COUNT_IF(src = 'PERM') AS perm_rows
    FROM lin_src
    GROUP BY ID_ITEM, ID_ORD, SEQ_LINE_ORD
)
SELECT
    COUNT(*) AS duplicate_keys_total,
    COUNT_IF(active_rows > 0 AND perm_rows > 0) AS keys_in_both_active_and_perm,
    COUNT_IF(active_rows > 1 AND perm_rows = 0) AS keys_duplicated_within_active,
    COUNT_IF(perm_rows > 1 AND active_rows = 0) AS keys_duplicated_within_perm
FROM k
WHERE rows_per_key > 1;
*/