-- Open Order Work Center Prices View in TCM
select distinct
	--OOwP Populate Cache
	getDate() as dataRefreshTimeStamp,
	ol.PRICE_NET as open_net_amt,
	oh.ID_CUST_SOLDTO as cust_soldto,
	sc.ship_complete_flag,
	ol.date_pick_last,
	case when ol.flag_pick=2 THEN CAST(nsa.WORKINGDAYSBETWEEN(ol.DATE_PICK_LAST, getdate()) as varchar) ELSE '' END as WorkingDaysSinceLastPicked,
	case when ol.flag_pick=2 then 'P' else '' end as flag_pick,
	case when oh.flag_ackn=2 then 'A' else '' end as flag_ack,
	oh.amt_ord_total,
	oh.id_slsrep_1,
	sv.DESCR_SHIP_VIA,
	Convert(DATETIME,ol.DATE_RQST,1) as DR, 
	Convert(DATETIME,ol.DATE_PROM,1) as DP,
	Convert(DATETIME,oh.date_ord,1) as DO, 
	CASE
		WHEN ol.DATE_RQST = ol.DATE_PROM THEN Convert(DATETIME,ol.DATE_RQST,1)
		WHEN oh.ID_CUST_SOLDTO = '102340' THEN ol.DATE_PROM
		WHEN ((ol.FLAG_STK = 'S' OR ilPAR.ID_PLANNER in ('AS','1A','KT','A ')) AND nsa.AddWorkDays(3,oh.DATE_ORD) >= ol.DATE_PROM) THEN nsa.AddWorkDays(1,ol.DATE_ADD)
		ELSE ol.DATE_PROM
	END as DATE_CALC_START,

	CASE
		WHEN ol.DATE_RQST = ol.DATE_PROM THEN Convert(DATETIME,ol.DATE_RQST,1)
		WHEN oh.ID_CUST_SOLDTO = '102340' THEN nsa.AddWorkDays(10,ol.DATE_PROM)
		WHEN ((ol.FLAG_STK = 'S' OR ilPAR.ID_PLANNER in ('AS','1A','KT','A ')) AND nsa.AddWorkDays(3,oh.DATE_ORD) >= ol.DATE_PROM) THEN ol.DATE_PROM
		ELSE nsa.AddWorkDays(10,ol.DATE_PROM)
	END as DATE_CALC_END,
	ol.id_item, 
	ps.id_item_comp, 
	CASE
		WHEN ps.ID_ITEM_COMP is not null AND ol.FLAG_STK = 'N' THEN 'Y'
		WHEN ilPAR.flag_source = 'P' THEN 'Y'
		WHEN WPR_PND.SUM_QTY_ONORD is not NULL OR WPS_PND.SUM_QTY_ONORD is not NULL THEN 'Y'
		ELSE 'N'
	END as flag_show,
	CASE
		WHEN sh.id_buyer = 'AS'  and il.qty_onhd is not null and ir.level_rop > 1 THEN 'AS'
		WHEN sh.id_buyer = '1A'  and il.qty_onhd is not null and ir.level_rop > 1 THEN 'AS'
		WHEN sh.id_buyer = 'KT'  and il.qty_onhd is not null and ir.level_rop > 1 THEN 'KT'
		ELSE ''
	END as alt_stk,
	ilPAR.ID_PLANNER as ilPAR_ID_PLANNER,
	oh.id_ord,
	oh.id_user_add,
	ol.date_add,
	ol.seq_line_ord, 
	ol.ID_SO as id_so_odbc, 
	rtrim(il.ID_LOC) as il_ID_LOC,
	rtrim(ol.ID_LOC) as ol_ID_LOC,
	mo.FLAG_MO,
	vert.VERTICAL, 
	case 
		when ib.code_comm like 'RM%' or ib.code_comm like 'FAB' or ib.code_comm like 'DF%' then '3-FABRIC'
		when (il.flag_stk = 'S' and ir.level_rop >1) or KIT_AS_flag_stk = 'STOCK' then '1-STOCK'
		else '2-MTO' 
	end as STOCK_STATUS,
	ib.code_cat_prdt,
	ib.code_user_1_im,
	ib.id_rev_draw,
	sv.ID_CARRIER,
	(ol.qty_open - isnull(sl.QTY_SHIP,0)) as qty_open, 
	ib.flag_stat_item, 
	ol.FLAG_STK as ol_FLAG_STK,
	il.FLAG_STK as il_FLAG_STK,
	case when ol.flag_stk = 'S' and isnull(LEVEL_ROP,0) =1 then 'M' else ol.flag_stk end as flag_stk, 
	CASE 
		WHEN rwc.RBN_WC = '3300' THEN 'JACKETS'
		WHEN rwc.RBN_WC = '3301' THEN 'POLOS'
		WHEN rwc.RBN_WC = '3302' THEN 'SHIRTS'
		WHEN rwc.RBN_WC = '3303' THEN 'TSHIRTS'
		WHEN rwc.RBN_WC = '3304' THEN 'FLEECE'
		WHEN rwc.RBN_WC = '3305' THEN 'ODDLOT'
		WHEN rwc.RBN_WC = '3306' THEN 'MASKS'
		WHEN rwc.RBN_WC = '3307' THEN 'GOWNS'
		ELSE '' 
	END AS RBN_WC,
	isnull(WPR.SUM_QTY_ONORD,0) as Qty_Rel,
	isnull(WPS.SUM_QTY_ONORD,0) - isnull(CASE WHEN PCC.stat_rec_so is null THEN WPS.SUM_QTY_ONORD else PCC.SUM_QTY_ONORD end,0) as Qty_Start,--Per Jeff 8/7/2025
	isnull(CASE WHEN PCC.stat_rec_so is null THEN WPS.SUM_QTY_ONORD else PCC.SUM_QTY_ONORD end,0) as Qty_presew,
	isnull(WPR_PND.SUM_QTY_ONORD,0) as Qty_Rel_PND,
	isnull(WPS_PND.SUM_QTY_ONORD,0) as Qty_Start_PND,
	isnull(il.QTY_ONHD,0) -isnull(tSBNB.SBNB,0) as QTY_ONHD,
	isnull(il.QTY_ALLOC,0) -isnull(tSBNB.SBNB,0) as QTY_ALLOC,
	isnull(il.QTY_ONORD,0) +isnull(ic.QTY_ONHD,0) as QTY_ONORD,
	isnull(tSBNB.SBNB,0) as SBNB,
	il.BIN_PRIM,
	il.FLAG_SOURCE, 
	il.FLAG_TRACK_BIN,
	isnull(ir.LEVEL_ROP,0) as LEVEL_ROP,
	case when isnull(ir.LEVEL_ROP,0)>1 and il.flag_stk = 'S' then 1 else 0 end as stk_test,
	ib.code_um_price, 
	oh.NAME_CUST as name_cust_soldto, 
	rtrim(ltrim(oh.ID_SLSREP_1)) as id_slsrep,
	sh.stat_rec_SO, 
	CASE
		when sh.stat_rec_so = 'S' and sh.DATE_START_OPER_1ST is null then 'R' 
		WHEN so3999.STAT_REC_OPER = 'C' and so9999.STAT_REC_OPER <> 'R' THEN 'W'
		WHEN so3999.STAT_REC_OPER is null and sh.stat_rec_so = 'S' and so9999.STAT_REC_OPER <> 'R' THEN 'W'
		WHEN so9999.STAT_REC_OPER = 'R' and ilPAR.id_planner not like 'KT' THEN 'D'
		ELSE sh.stat_rec_SO
	END as STAT_REC_SO_display,

	sh.id_SO, 
	ol.qty_ship_total, 
	sl.id_ship, 
	oh.code_stat_ord,
	CASE cs.STATUS_CREDIT 
		WHEN 0 THEN 'H'
		WHEN 1 THEN 'R'
	END as CREDIT_STATUS,
	oh.id_po_cust,
	sph.NUM_SHIPMENTS,
	ihh.NUM_INVCS,
	1 as COUNTER,
	rtrim(ol.id_loc) as id_loc


into nsa.OOWP_TEMP
	 
FROM 
	nsa.CP_ORDLIN ol WITH (NOLOCK)
LEFT JOIN
	(select oh.id_ord, cc.comment, flag_del,case when cc.comment like '%#MO%' then 'Y' else 'N' end as flag_MO 
	from 
		nsa.cp_ordhdr oh WITH (NOLOCK)
		left join 
			(select * from nsa.cp_ordhdr_custom_comments WITH (NOLOCK) where isnull(FLAG_DEL,'') <> 'D') cc
			on oh.id_ord = cc.id_ord) MO
	ON ol.id_ord = MO.id_ord
LEFT JOIN
	nsa.CP_ORDHDR oh WITH (NOLOCK)
	on oh.id_ord=ol.id_ord
LEFT JOIN
	(Select id_item, qty_onhd 
		from
		nsa.itmmas_loc 
	where id_loc = 'INTR'
	and qty_onhd <>0) ic
	on ol.id_item = ic.id_item
LEFT JOIN
	nsa.CP_CREDIT_STS cs WITH (NOLOCK)
	on oh.id_ord=cs.id_ord
	and cs.TYPE_REC = 0
LEFT JOIN
	(select distinct id_ord,'Y' as ship_complete_flag from nsa.CP_COMMENT WITH (NOLOCK)
	where note like '%SHIP%COMPLETE%' and note not like '%LINE%') sc
	on oh.id_ord = sc.id_ord
LEFT JOIN 
	(select id_item, val_string_attr as RBN_WC from nsa.im_cmcd_attr_value WITH (NOLOCK)
	where id_attr like 'RBN_WC' and val_string_attr<>'') rwc
	on ol.id_item = rwc.id_item
LEFT JOIN 
	nsa.shpord_hdr sh WITH (NOLOCK)
	ON ltrim(ol.id_so)=ltrim(sh.id_so)
	--and ltrim(ol.sufx_so)=ltrim(sh.sufx_so)
LEFT JOIN 
	nsa.CP_SHPLIN sl WITH (NOLOCK)
	ON ol.id_ord=sl.id_ord
	--and ol.id_item=sl.id_item
	and ol.seq_line_ord=sl.seq_line_ord
	--and rtrim(sl.id_loc) like @location
	and rtrim(sl.ID_LOC) = rtrim(ol.ID_LOC)
LEFT JOIN 
	(select ps2.* from nsa.PRDSTR ps2 WITH (NOLOCK)
	left join nsa.itmmas_base ib2 WITH (NOLOCK)
	on ps2.id_item_comp = ib2.id_item
	where ib2.code_comm = 'FG') ps

	on ol.id_item = ps.ID_ITEM_PAR
	and ol.flag_stk = 'N'
	and (ps.ID_ITEM_COMP = replace(ol.id_item,'*','') 
		OR ps.ID_ITEM_COMP like 'PNT%UI%' 
		OR ps.ID_ITEM_COMP like 'SPX%' 
		OR ps.ID_ITEM_COMP like 'TCG%' 
		OR ps.ID_ITEM_COMP like 'SHRDR3%' 
		OR ps.ID_ITEM_COMP like 'C54WFLS%' 
		OR ps.ID_ITEM_COMP like 'C54VYLS%' 
		OR ps.ID_ITEM_COMP like 'DF2-CM-618-JN-DN-%' 
		OR ps.ID_ITEM_COMP like 'HYDROJACK%'
		OR sh.ID_BUYER = 'AS'
	)
	and ((getdate() between ps.DATE_EFF_START and ps.DATE_EFF_END) OR ps.DATE_EFF_START is null OR ps.DATE_EFF_END is null)
LEFT JOIN 
	nsa.ITMMAS_LOC il WITH (NOLOCK)
	on il.ID_ITEM = --ol.id_item 
	CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	--and rtrim(il.ID_LOC) like @location
	and rtrim(il.ID_LOC) = rtrim(ol.ID_LOC)
LEFT JOIN 
	nsa.ITMMAS_LOC ilPAR WITH (NOLOCK)
	on ilPAR.ID_ITEM = ol.id_item 
	--and rtrim(ilPAR.ID_LOC) like @location
	and rtrim(ilPAR.ID_LOC) = rtrim(ol.ID_LOC)
LEFT JOIN
	nsa.ITMMAS_BASE ib WITH (NOLOCK)
	on ilPAR.ID_ITEM = ib.ID_ITEM
LEFT JOIN 
	nsa.ITMMAS_REORD ir WITH (NOLOCK)
	on il.ID_ITEM = ir.ID_ITEM
	and il.ID_LOC = ir.ID_LOC_HOME
LEFT JOIN 

	(select ID_ITEM, ID_LOC, sum(QTY_SHIP) as SBNB from nsa.CP_SHPLIN WITH (NOLOCK) where FLAG_CONFIRM_SHIP <> 1 group by ID_ITEM, ID_LOC) as tSBNB
	on ol.ID_LOC = tSBNB.ID_LOC
	and ol.id_item = --tSBNB.id_item
	CASE
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE tSBNB.id_item
	END
LEFT JOIN  
	nsa.tables_code_ship_via_cp sv WITH (NOLOCK)
	on oh.CODE_SHIP_VIA_CP = sv.CODE_SHIP_VIA_CP 

LEFT JOIN
    (select ID_ORD, count(ID_SHIP) as NUM_SHIPMENTS from nsa.CP_SHPHDR WITH (NOLOCK) group by ID_ORD) as sph
    on oh.ID_ORD = sph.ID_ORD
LEFT JOIN
    (select ID_ORD, count(ID_INVC) as NUM_INVCS from nsa.CP_INVHDR_HIST WITH (NOLOCK) group by ID_ORD) as ihh
    on oh.ID_ORD = ihh.ID_ORD
LEFT JOIN 
	(select id_loc, STAT_REC_SO, 
	sum(QTY_ONORD) as SUM_QTY_ONORD,
	ID_ITEM_PAR
	from nsa.SHPORD_HDR sh WITH (NOLOCK)
	where sh.STAT_REC_SO in ('R')
	group by ID_ITEM_PAR, STAT_REC_SO, id_loc
	) WPR
	on WPR.ID_ITEM_PAR = --pd.id_item 
	CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	and ol.id_loc = WPR.id_loc
LEFT JOIN
	(select sh.id_loc, STAT_REC_SO, 
	sum(QTY_ONORD) as SUM_QTY_ONORD,
	ID_ITEM_PAR
	from nsa.SHPORD_HDR sh WITH (NOLOCK)
	left join
	nsa.shpord_oper so with (NOLOCK)
	on ltrim(sh.id_so) = ltrim(so.id_so)
	and ltrim(sh.sufx_so) = ltrim(so.sufx_so)
	where sh.STAT_REC_SO in ('S') and so.id_oper = 3999 and so.stat_rec_oper = 'C'
	group by ID_ITEM_PAR, STAT_REC_SO, sh.id_loc
	) PCC
	on PCC.id_item_par = 
		CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	and ol.id_loc = PCC.id_loc
LEFT JOIN 
	(select sh.id_loc, STAT_REC_SO, 
	sum(QTY_ONORD) as SUM_QTY_ONORD,
	ID_ITEM_PAR
	from nsa.SHPORD_HDR sh WITH (NOLOCK)
	where sh.STAT_REC_SO in ('S')
	group by ID_ITEM_PAR, STAT_REC_SO,sh.id_loc
	) WPS
	on WPS.ID_ITEM_PAR = --pd.id_item 
	CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	and ol.id_loc = WPS.id_loc

LEFT JOIN
	(select sh.id_loc, STAT_REC_SO, 
	sum(QTY_ONORD) as SUM_QTY_ONORD,
	ID_ITEM_PAR,
	replace(ID_ITEM_PAR,'#','') as ID_ITEM_PAR_NP
	from nsa.SHPORD_HDR sh WITH (NOLOCK)
	where sh.STAT_REC_SO in ('R')
	and sh.ID_ITEM_PAR like '%#'
	group by ID_ITEM_PAR, STAT_REC_SO, sh.id_loc
	) WPR_PND
	on WPR_PND.ID_ITEM_PAR_NP = 
	CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	and ol.id_loc = WPR_PND.id_loc
LEFT JOIN
	(select sh.id_loc,STAT_REC_SO, 
	sum(QTY_ONORD) as SUM_QTY_ONORD,
	ID_ITEM_PAR,
	replace(ID_ITEM_PAR,'#','') as ID_ITEM_PAR_NP
	from nsa.SHPORD_HDR sh WITH (NOLOCK)
	where sh.STAT_REC_SO in ('S')
	and sh.ID_ITEM_PAR like '%#'
	group by ID_ITEM_PAR, STAT_REC_SO, sh.id_loc
	) WPS_PND
	on WPS_PND.ID_ITEM_PAR_NP = 
	CASE 
		WHEN ps.ID_ITEM_COMP is not null THEN ps.ID_ITEM_COMP
		ELSE ol.id_item
	end
	and ol.id_loc = WPS_PND.id_loc

LEFT JOIN
	nsa.SHPORD_OPER so9999 WITH (NOLOCK)
	on sh.ID_SO = so9999.ID_SO
	and sh.SUFX_SO = so9999.SUFX_SO
	and ltrim(so9999.ID_OPER) = '9999'

LEFT JOIN
	nsa.SHPORD_OPER so3999 WITH (NOLOCK)
	on sh.ID_SO = so3999.ID_SO
	and sh.SUFX_SO = so3999.SUFX_SO
	and ltrim(so3999.ID_OPER) = '3999'

--LEFT JOIN 
--	nsa.CP_ORDHDR_CUSTOM_COMMENTS ohcc WITH (NOLOCK)
--	on oh.ID_ORD = ohcc.ID_ORD
--	and isnull(ohcc.FLAG_DEL,'') <> 'D'


LEFT JOIN
      (select 
		ib.ID_ITEM,
		case when code_cat_prdt in ('05','06') then 'FR CLOTHING'
		when code_cat_prdt = '10' then 'ARC FLASH PPE'
		when code_cat_prdt = '11' then 'KUNZ'
		when code_cat_prdt = '15' then 'THERMAL'
		when code_cat_prdt = '20' then 'INDUSTRIAL PPE'
		when code_cat_prdt in ('25','26') then 'MILITARY'
		when code_cat_prdt = '30' then 'USPS'
		when code_cat_prdt = '35' then 'AD SPECIALTY'
		when code_cat_prdt = '40'  then 'LAW ENFORCEMENT'
		when code_cat_prdt = '45'  then 'GOVERNMENT - NON MILITARY'
		else 'INDUSTRIAL PPE' end as VERTICAL
		FROM nsa.itmmas_base ib  WITH (NOLOCK)
		) vert 
on vert.ID_ITEM = ol.ID_ITEM

left join
	(select 
		chk.id_item_par, 
		min(chk.KIT_AS_flag_stk) as KIT_AS_flag_stk
		--min(chk.comp_OH_AVAIL) as comp_OH_AVAIL,
		--min(chk.qty_onord) as comp_onord,
		--min(chk.level_rop) as comp_level_rop
	from 
		(
		select 
			ps.id_item_par, ps.id_item_comp, 
			il.qty_onhd-il.qty_alloc as comp_OH_AVAIL,
			il.qty_onord,
			ro.level_rop,
			case when il.flag_stk= 'S' and ro.level_rop >1 then 'STOCK' else 'MTO' end as KIT_AS_flag_stk
			
		from 
			nsa.prdstr ps  WITH (NOLOCK)
		left join 
			nsa.itmmas_loc il  WITH (NOLOCK)
			on ps.id_item_comp = il.id_item
			and il.id_loc = '10'
		left join 
			nsa.itmmas_loc il2  WITH (NOLOCK)
			on ps.id_item_par = il2.id_item
		left join 
			nsa.itmmas_reord ro  WITH (NOLOCK)
			on il.id_item = ro.id_item
			and il.id_loc = ro.id_loc_home
		where 
			il2.id_planner in ('1A','AS','KT') and 
			ps.date_eff_end ='9999-12-31 00:00:00.000'
		) chk

		group by chk.id_item_par) chk
	on ib.id_item = chk.id_item_par



WHERE 

oh.date_ord is not NULL
and (sl.id_ship is NULL OR sl.QTY_OPEN > sl.QTY_SHIP)
and (sh.SUFX_SO = 0 OR sh.SUFX_SO is null)

