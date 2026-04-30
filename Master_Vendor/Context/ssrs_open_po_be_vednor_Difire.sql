/*
poh = PORHDR_HDR
pol = PORLIN_ITEM
pop = porlin_outp
idb = tables_id_buyer_po
tl = tables_loc
il = ITMMAS_LOC

*/
SELECT DISTINCT
	poh.ID_PO, 
	isnull(pol.ID_LINE_PO,pop.ID_LINE_PO) as ID_LINE_PO, 
	poh.id_vnd_payto,
	poh.ID_LOC_SHIPTO,
	case when poh.ID_LOC_SHIPTO = '10' THEN 'NSA - CLEVELAND'
	when poh.ID_LOC_SHIPTO = '20' THEN 'NSA - CHICAGO'
	when poh.ID_LOC_SHIPTO = '40' THEN 'AG SAFETY'
	when poh.ID_LOC_SHIPTO = '50' THEN 'NSA - ARKANSAS'
	else tl.DESCR
	END as Loc_Description,
	case when trim(poh.type_po) ='' then 'BLANKET'
		when isnull(pol.type_rec,pop.type_rec) = 1 then 'NORMAL' 
		when isnull(pol.type_rec,pop.type_rec) = 3 then 'OUTP' 
		 else '' end as PO_TYPE,
	isnull(pol.ID_ITEM,pop.id_item_outP) as ID_ITEM, 
	isnull(ID_ITEM_VND,code_proc_op) as ID_ITEM_VND, 
	poh.NAME_VND_ORDFM, 
	isnull(pol.DATE_PROM,pop.DATE_PROM) as DATE_PROM, 
	isnull(pol.DATE_RQST,pop.DATE_RQST) as DATE_RQST, 
	isnull(pol.CODE_UM_PUR, pop.CODE_UM_PUR) as CODE_UM_PUR, 
	isnull(pol.QTY_ORD, pop.QTY_ORD) as QTY_ORD, 
	isnull(pol.QTY_RCV, pop.QTY_RCV) as QTY_RCV,
	isnull(pol.QTY_ORD - pol.QTY_RCV,pop.QTY_ORD - pop.QTY_RCV) as QTY_DUE, 
	poh.DATE_PO,
	isnull(pol.cost_expect,pop.cost_expect) as COST_EXPECT,
	isnull(pol.DATE_ADD,pop.DATE_ADD) as DATE_ADD,
	poh.ID_BUYER,
	idb.DESCR as NAME_BUYER,
	isnull(pol.CSTM_DATE_1, pop.CSTM_DATE_1) as CRNT_DATE,
	isnull(pol.CSTM_DATE_2, pop.CSTM_DATE_2) as CRNT_DELIVERY
FROM 
	nsa.PORHDR_HDR poh
left join 
	nsa.PORLIN_ITEM pol
	on poh.ID_PO = pol.ID_PO
	and poh.id_rel_ord = pol.id_rel_ord
left join
	nsa.porlin_outp pop
	on poh.ID_PO = pop.ID_PO
	and poh.id_rel_ord = pop.id_rel_ord
left join 
	nsa.tables_id_buyer_po idb
	on poh.ID_BUYER = idb.ID_BUYER
left join
	nsa.tables_loc tl
	on poh.ID_LOC_SHIPTO = tl.ID_LOC
left join 
	nsa.ITMMAS_LOC il
	on pol.ID_ITEM = il.ID_ITEM
WHERE 
	(poh.FLAG_STAT_PO not in ('C', 'X') ) 
	and (isnull(pol.FLAG_STAT_LINE_PO,pop.FLAG_STAT_LINE_PO)!= 'C') 
	and isnull(pol.QTY_RCV,pop.QTY_RCV)< isnull(pol.QTY_ORD,pop.QTY_ORD)
	-- and (ltrim(poh.ID_VND_ORDFM) = @vendorID OR @vendorID is null) 
	-- and (isnull(pol.DATE_PROM,pop.DATE_PROM) <= @dateEndProm OR @dateEndProm is null) 
	-- and poh.id_buyer in (@ID_PLANNER)
	-- and poh.id_loc_shipto in (@loc)
	-- and (pol.id_item like @id_item or pop.id_item_outp like @id_item)
order by 
	poh.NAME_VND_ORDFM, 
	poh.ID_LOC_SHIPTO,
	isnull(pol.ID_ITEM,pop.id_item_outP),
	isnull(pol.DATE_ADD,pop.DATE_ADD)