--declare @IdVend as varchar(100) = '340401'
-----------------------------------------------------------------------------
--08-24-16 - RB:	Added Purchase Cost per Sharons request
--09-29-22 - JDM:	Changed to payto vendor instead of order from per TJB
--11-12-25 - JDM:	Included vendor flag per Andrea
-----------------------------------------------------------------------------
select 
	vo.ID_VND,
	vo.NAME_VND,
	iv.flag_vnd_prim,
	iv.ID_ITEM as NSA_ID_ITEM,
	iv.ID_ITEM_VND as VND_ID_ITEM,
	concat(ib.DESCR_1,ib.DESCR_2) as DESCR,
	--(ic.COST_TOTAL_ACCUM_CRNT * ib.RATIO_STK_PUR )as Cost
	cast(substring(iv.PRICE_VND_FC_1,5,8) as decimal(10,0))*.01 as Cost
from 
	nsa.ITMMAS_VND iv
left join 
	nsa.VENMAS_PAYTO vo
	on ltrim(iv.ID_VND_PAYTO) = ltrim(vo.ID_VND)
left join 
	nsa.ITMMAS_BASE ib
	on iv.ID_ITEM = ib.ID_ITEM
left join 
	nsa.ITMMAS_COST ic
	on iv.ID_ITEM = ic.ID_ITEM
where 
	ltrim(vo.ID_VND) = @IdVend
	and ib.FLAG_STAT_ITEM = 'A'
order by 
	NSA_ID_ITEM asc