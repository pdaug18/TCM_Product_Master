select distinct
	--OOwP_CACHED
	cache.dataRefreshTimeStamp,
	cache.open_net_amt,
	cache.cust_soldto,
	cache.ship_complete_flag,
	cache.WorkingDaysSinceLastPicked,
	cache.flag_pick,
	cache.flag_ack,
	cache.amt_ord_total,
	cache.id_slsrep_1,
	cache.ID_CARRIER,
	cache.DESCR_SHIP_VIA,
	cache.DR, 
	cache.DP,
	cache.DO, 
	cache.DATE_CALC_START,
	cache.DATE_CALC_END,
	cache.ilPAR_ID_PLANNER,
	cache.id_item, 
	cache.id_item_comp, 
	cache.flag_show,
	cache.alt_stk,
	cache.id_ord,
	cache.id_user_add,
	cache.date_add,
	cache.seq_line_ord, 
	cache.id_so_odbc, 
	cache.il_ID_LOC,
	cache.ol_ID_LOC,
	cache.FLAG_MO,
	cache.code_cat_prdt,
	cache.code_user_1_im,
	cache.qty_open, 
	cache.flag_stat_item, 
	cache.ol_FLAG_STK,
	cache.flag_stk, 
	cache.Qty_Rel,
	cache.Qty_Start,
	cache.Qty_presew,
	cache.Qty_Rel_PND,
	cache.Qty_Start_PND,
	cache.QTY_ONHD,
	cache.QTY_ALLOC,
	cache.QTY_ONORD,
	cache.SBNB,
	cache.BIN_PRIM,
	cache.LEVEL_ROP,
	cache.stk_test,
	cache.code_um_price, 
	cache.name_cust_soldto, 
	cache.id_slsrep,
	cache.stat_rec_SO, 
	cache.STAT_REC_SO_display,
	cache.id_SO, 
	cache.qty_ship_total, 
	cache.id_ship, 
	cache.code_stat_ord,
	cache.CREDIT_STATUS,
	cache.id_po_cust,
	cache.NUM_SHIPMENTS,
	cache.NUM_INVCS,
	cache.COUNTER,
	cache.id_rev_draw,
	cache.id_loc,
	cache.VERTICAL,
	Convert(DATETIME,ohcc.DATE_EST_SHIP,1) as DATE_EST_SHIP, 
	Convert(DATETIME,ohcc.DATE_OLD_SHIP,1) as DATE_OLD_SHIP, 
	ohcc.COMMENT
/*	cache.id_wc,
	cache.descr_wc*/

FROM nsa.OOWP_TEMP cache

LEFT JOIN 
	nsa.CP_ORDHDR_CUSTOM_COMMENTS ohcc WITH (NOLOCK)
	on cache.ID_ORD = ohcc.ID_ORD
	and isnull(ohcc.FLAG_DEL,'') <> 'D'


WHERE 
	cache.DO is not NULL
    and cache.id_ord = '863767'