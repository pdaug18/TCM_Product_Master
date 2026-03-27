USE [TCM101]
GO

/****** Object:  View [nsa].[RPT50_ORDLIN_PromDt]    Script Date: 3/27/2026 8:46:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO


-- create view [nsa].[RPT50_ORDLIN_PromDt]
-- as

    select

        --columns from CP_ORDLIN

        --Added by WDK 11-6-06
        ---JRH t1.company_code,        --Company Code

        t1.amt_commsn, --Commision Amount
        --
        CONVERT(CHAR,t1.date_add,112) 
AS date_add_ordlin, -- Date record added
        CONVERT(CHAR,t1.date_prom,112) AS DATE_PROM, --Date promised (ANCHOR)

        --Convert Anchor Date from DateTime to CCYYMMDD (needed to drop the time 
        --portion).  Configure the Site to Date Type=8.
        CONVERT(CHAR,DATE_RQST,112) AS DATE_RQST, --Date requested

        t1.id_config, --Configuration ID
        t1.id_est as id_est_odbc, --Estimate number
        t1.id_item, --Item ID
        t1.id_item_cust, --Customer Item number
        t1.id_loc, --Location
        t1.id_ord, --Customer order number
        t1.id_quote as id_quote_odbc, --Quote number
        t1.id_so as id_so_odbc, --Shop order number
        t1.qty_bo, --Backorder Quantity
        t1.qty_book, --Booking Quantity
        t1.qty_open, --Open Quantity
        --Calculated facts

        --Changes done to accomadate the calc fields have been removed, so we have to convert the varchar field to an integer.
        --Changed WDK 6-11-08

        t1.qty_open *   convert(int,(substring(cost_unit_vp,7,10))) / power(10,convert(int,(substring(cost_unit_vp, 1, 1)))+2) as open_cost, --Open Cost
        t1.qty_open *   convert(int,(substring(price_sell_net_vp,7,10))) / power(10,convert(int,(substring(price_sell_net_vp, 1, 1)))+2) as open_NET_amt, --Open NET Amount
        t1.qty_open * convert(int,(substring(price_list_vp,7,10))) / power(10,convert(int,(substring(price_list_vp, 1, 1)))+2) as open_list_amt, --Open list Amount
        -- Chg WDK 3-10-07 & 6-11-08
        ((t1.qty_open * convert(int,(substring(price_sell_net_vp,7,10))) / power(10,convert(int,(substring(price_sell_net_vp, 1, 1)))+2)) -
(t1.qty_open * convert(int,(substring(cost_unit_vp,7,10)))/ power(10,convert(int,(substring(cost_unit_vp, 1, 1)))+2))) as open_margin, --Open Margin
        --


        t1.qty_org, --Original order Quantity
        t1.qty_rel, --Qty selected on the shipping schedule
        t1.qty_ship_total, --Total number of units shipped
        t1.seq_line_ord, --Line Sequence number
        t1.seq_rev_quote, --Quote Revision Sequence Number
        t1.sufx_so, --Created shop order suffix
        t1.ver_bo, --backorder version
        t1.code_cat_prdt, --Product Category Code

        --columns from CP_ORDHDR
        t2.code_cust_1, --Customer type
        t2.code_cust_2, --Sold to <User #1>
        t2.code_cust_3, --Ship to <User #1>
        t2.code_ship_via_cp, --Ship Via
        t2.code_stat_ord, --Order status
        t2.code_trms_cp, --Terms code
        --
        CONVERT(CHAR,t2.date_add,112) 
AS date_add_ordhdr, --Date record added
        --
        CONVERT(CHAR,t2.date_ord,112) 
AS date_ord, --Order Date
        t2.id_cust_billto as id_cust_billto_odbc, --Bill-to Customer number
        t2.id_job, --Job number
        t2.id_ord as id_ord2, --Customer Order number
        t2.id_po_cust, --Customer PO Number
        t2.id_quote as id_quote_odbc2, --Quote Number
        t2.id_slsrep_1, --Sales Rep 1 Historical
        t2.seq_shipto, --Ship-to sequence number
        t2.type_ord_cp, --Order type

        --concat prod_cat/cust_type
        t1.code_cat_prdt + t2.code_cust_1 as concat_prod_cat,

        --columns from RPT40_CUSMAS
        t3.code_ship_via_cp as code_ship_via_cp2, --Ship Via
        t3.descr_ship_via, --Ship via description
        t3.code_trms_cp as code_trms_cp2, --Terms code
        t3.descr_trms, --Terms code description
        t3.code_user_1_ar , --User Defined Code 1
        t3.code_user_2_ar , --User Defined Code 2
        t3.code_user_3_ar , --User Defined Code 3
        t3.id_cust_billto as cust_soldto, --Bill-to Customer number (Soldto?)
        t3.id_cust as cust_shipto, --sold to customer(Shipto?)

        --
        --Changed by WDk 12-05-07 
        t3.id_slsrep , --Sales Representative current
        t3.name_slsrep as name_slsrep_curr, --Sales Representative name Current
        --

        t3.name_cust_soldto , --sold to customer name
        t3.name_cust_shipto, --ship to customer name
        t3.seq_shipto as seq_shipto2, --Ship-to sequence number
        t3.ship_city, --shipto city
        t3.ship_country, --country
        t3.ship_id_st, --state
        t3.ship_prov, --province
        t3.ship_zip, --zip
        t3.sold_city, --soldto city
        t3.sold_country, --country
        t3.sold_id_st, --state
        t3.sold_prov, --province
        t3.sold_zip, --zip
        --t3.code_cust,		--Customer type --REMOVE PER BILL K. 7/1/2004 - KJM

        --
        --Added by WDK 12-05-07 get slsrep name from table_slsrep
        t8.name_slsrep as name_slsrep_hist, --Sales Representative name Historical

        --columns from RPT40_ITMMAS
        t4.bin_prim, --Primary Bin
        t4.buyer_name_desc, --Buyer Name
        t4.code_abc, --Inventory class (A, B or C)
        t4.code_cat_cost, --Cost category
        t4.code_comm, --Commodity code
        t4.code_frt, --Freight Code
        t4.code_um_price, --Pricing Unit of Measure
        t4.code_um_pur, --Purchasing unit of measure
        t4.code_um_stk, --Stocking Unit of Measure
        t4.code_um_vnd, --Vendor's Unit of Measure
        t4.code_user_1_im, --User Defined Code 1
        t4.code_user_2_im, --User Defined Code 2
        t4.code_user_3_im, --User Defined Code 3
        t4.comm_code_desc, --Commodity Code
        t4.cost_category_desc, --Cost Category
        t4.date_add as date_add_itmmas, --Date record added
        t4.date_chg, --Date record changed
        t4.date_chg_qty_onhd, --Date on-hand quantity changed
        t4.date_cnt_last, --Date last counted (CCYYMMDD)
        t4.date_iss_last, --Date last issued or sold
        t4.date_rcv_last, --Date last received or purchase
        t4.descr_1, --Description - 1st
        t4.descr_2, --Description - 2nd
        t4.descr_type_cost, --Description - Cost Type
        t4.flag_cntrl, --Controlled/Noncontrolled flag
        t4.FLAG_SOURCE, --Purchased/Manufactured flag
        t4.flag_plcy_ord, --Order Policy Flag
        t4.flag_stat_item, --Status Flag
        t4.flag_stk, --Stocked/Nonstocked Flag
        t4.flag_track_bin, --Item bin/lot/serial tracked?
        t4.flag_vnd_prim, --Primary or secondary vendor
        t4.frt_code_desc, --Freight Code
        t4.home_loc_desc, --Home Location
        t4.home_loc_type_desc, --Home Location Type
        t4.ID_PLANNER, --Buyer/Analyst
        t4.id_item_vnd, --Vendor item number
        t4.id_loc_home, --Home Location
        t4.id_prdtline, --Product line ID
        t4.id_rte, --Routing number
        t4.id_vnd_ordfm, --Buy from Vendor
        t4.id_vnd_payto, --Pay to Vendor

        --added WDK 12-29-06
        t4.ProdCatCd_Desc, -- Product Cat Code Descr

        t4.status_flag_desc, --Status Flag
        t4.type_cost, --Cost type
        t4.venitmnbr_desc, --Vendor Item Number

        --columns from TABLES_LOC 
        t5.descr as ol_loc_desc, --Order Line Location

        --COLUMNS FROM TABLES_CODE_CAT_PRDT
        CASE
WHEN T6.DESCR IS NOT NULL THEN T6.DESCR ELSE T7.DESCR 
END AS PROD_CAT_DESCR,

        (t1.qty_open * t4.cost_total_accum_std) as ext_std_open_cst, --extended std open cost
        (t1.qty_ship_total * t4.cost_total_accum_std) as ext_std_invc_cst, --extended std ship/invoiced cost

        (t1.qty_open * t4.cost_total_accum_crnt) as ext_cur_open_cst, --extended curr open cost
        (t1.qty_ship_total * t4.cost_total_accum_crnt) as ext_cur_invc_cst, --extended curr ship/invoiced cost

        --****************************************************************--
        -- **  Dates Dims for SQL Server DateTime Columns
        --****************************************************************--

        --Year
        YEAR(T1.DATE_PROM)
AS DATE_PROM_YEAR,

        --Year/Qtr
        CONVERT(INT, YEAR(T1.DATE_PROM)) * 10 + CONVERT(INT, 
  DATEPART(QQ, T1.DATE_PROM))
AS DATE_PROM_YEARQTR,

        --Year/Month
        CONVERT(INT,YEAR(DATE_PROM)) * 100 +
CONVERT(INT, MONTH(DATE_PROM)) 
AS DATE_PROM_YEARMONTH,

        --Year/Week
        --Special Note: the YEAR must be returned from the Sunday Date
        --              of the week, so need to subtract (Day of Week + 1)
        --              before getting the Year.  Example is Saturday,   
        --              January 1, 2000 which should return Week 52, year 1999
        YEAR(DATE_PROM - (DATEPART(DW,DATE_PROM)-1)) *100 +
DATEPART(WK,DATE_PROM - (DATEPART(DW, DATE_PROM)-1)) 
AS DATE_PROM_YEARWEEK,

        --DayOfWeek
        DATEPART(DW,DATE_PROM)  
AS DATE_PROM_DAYOFWEEK,

        --Month
        MONTH(DATE_PROM) 
AS DATE_PROM_MONTH,

        --FISCAL PERIOD SUPPORT FROM RPT30_CLYRPD
        GFFSYR, --FISCAL PERIOD
        GFFYPD, --FISCAL YEAR
        GFFYQT, --FISCAL QUARTER

        --FISCAL DATE DIMENSIONS - SQL SERVER
        substring(convert(char, GFFSYR),1,4) as GDFY,
        substring(convert(char, GFFYQT),1,4) + '-' + 
substring(convert(char, GFFYQT),5,2) as GDFYQT,
        substring(convert(char, GFFYPD),1,4) + '-' + 
substring(convert(char, GFFYPD),5,2) as GDFYPD

    from nsa.CP_ORDLIN t1 with(nolock)

        left outer join nsa.CP_ORDHDR t2 with(nolock) on 
  t1.id_ord = t2.id_ord
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t2.company_code


        left outer join nsa.RPT40_CUSMAS t3 with(nolock) on
  (t2.id_cust_soldto = t3.id_cust
            and t2.seq_shipto = t3.seq_shipto)
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t3.company_code


        left outer join nsa.RPT40_ITMMAS t4 with(nolock) on
(t1.id_item = t4.id_item
            and t1.id_loc = t4.id_loc)
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t4.company_code


        left outer join nsa.TABLES_LOC t5 with(nolock) on
  t1.id_loc = t5.id_loc
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t5.company_code


        LEFT OUTER JOIN nsa.TABLES_CODE_CAT_PRDT T6 with(nolock) ON
(t1.code_cat_prdt = t6.code_cat_prdt)
            AND
            (T2.CODE_CUST_1 = T6.CODE_TYPE_CUST)
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t6.company_code


        LEFT OUTER JOIN nsa.TABLES_CODE_CAT_PRDT T7 with(nolock) ON
(t1.code_cat_prdt = t7.code_cat_prdt)
            AND (T7.CODE_TYPE_CUST = ' ')
        -- added by WDK 11-6-06
        ---JRH and t1.company_code = t7.company_code

        --
        -- Added by WDK 12-05-07 join to table_slsrep

        LEFT OUTER JOIN nsa.TABLES_SLSREP T8 with(nolock) ON
(t2.id_slsrep_1 = t8.id_slsrep)
        ---JRH and t2.company_code = t8.company_code


        LEFT OUTER JOIN nsa.RPT35_CLYRPD WITH(NOLOCK)
        ON CONVERT(CHAR,date_prom,112) >= BEG_DATE
            AND CONVERT(CHAR,date_prom,112) <= END_DATE
 
GO