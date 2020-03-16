create view vHabitoFINAL as 
select DISTINCT periodo, idenCuenta, case when TIPOPAGO is null then 6
						when TIPOPAGO = '< MINIMO' then 1
						when TIPOPAGO = '= MINIMO' or TIPOPAGO = ' = MINIMO' then 2
						when TIPOPAGO = '> MINIMO' then 3
						when TIPOPAGO = 'FULL - PAGO MES' then 4
						when TIPOPAGO = 'FULL - PAGO TOTAL' then 5
						when TIPOPAGO = 'NO PAGO'then 0 end as HabitoP
From [DW_IC].[dbo].[HISTORICO_PAGOS_BF]
where periodo >=  201810 and periodo <= 201909


-----------------------------------------------------------------------------------------------------
---modificar 201908 y 201808



with vHabitoPago as
(
SELECT *
FROM
(
SELECT DISTINCT idencuenta,PERIODO, HabitoP  FROM vHabitoFINAL
) AS SourceTable PIVOT( MAX(HABITOP)
 FOR PERIODO IN([201810],[201811],[201812],[201901], [201902], [201903], [201904],[201905], [201906], [201907], [201908])) AS PivotTable
 ), tmpPeriodo as(
select *From [OR_TMP_REPORT_PROV_2_2]
where periodo =201908
), tmpTotal201908 as( 
select a.idencuenta, a.numdoc, a.sexo, a.edad, b.estadocivil, b.departamento, s_fin_estab, s_rev_estab, s_fin_atm,
s_rev_atm, saldo_lp, b.saldo_mora, saldo_exceso, dias_atraso as DiasMora201908,
 a.estado, b.nombre_producto, a.linea_credito_usd,
a.Linea_Credito_USD * a.tipocambio as  linea_credito_soles, flg_situacionlaboral_oscar, porc_uso_linea, flg_ppd, 
flg_usodisef, zona_apein
 From mae_titular_uba_201908 a inner join tmpPeriodo b on b.id_cuenta = a.idencuenta
), tmpRcc as
(
select  NroDocumento, maxlinea_bench, case when s_castigos >0 then 1 else 0 end [FlagScastigado]
, case when s_refinanciados > 0 then 1 else 0 end [FlagRefinanciado]
,case when PLD > 0 then 1 else 0 end [FlagPLD]
,CASE WHEN SALDO_DISEF_TC_CONSUMO > 0 THEN '1' ELSE '0' END [FlagDisefTcExterno]
, case when linea_Tc = 0 then '0 Linea_tc'
								when saldo_tc / LINEA_TC < 0.25 then 'a) <0% - 25%] uso línea'
								when saldo_tc / LINEA_TC < 0.5 then 'b) <25% - 50%] uso línea'
								when saldo_tc / LINEA_TC < 0.75 then 'c) <50% - 75%] uso línea'
								when saldo_tc / LINEA_TC < 1 then 'd) <75% - 100%] uso línea'
								when saldo_tc / linea_tc >= 1 then 'e) 100% a más uso línea'
								else 'Sin Inf' end [FlgRCC]
,case   when Linea_TC <= 4000  then  'D'
                                when Linea_TC <= 8000  then  'C2'
                                when Linea_TC <= 15000 then 'C1'
                                when Linea_TC <= 25000 then 'B2'
                                when Linea_TC <= 45000 then 'B1'
                                when Linea_TC <= 60000 then 'A2'
                                else 'A1' end [SegmentoNSE]
from OR_TMP_MAX_LINEA_RCC
where periodo = 201908
), tmpConsumo as
(
select A.idencuenta, --max(consumoMensual) as [consumoMaxMensual],
sum(ImporteSoles) as [ConsumoAnual201808_201908],
count(ImporteSoles) as [NroTransaccionAnual201808_201908]
FROM mtransaccion_uba A --a join VTRANS  b on a.IdenCuenta = b.IdenCuenta
where   periodotx >= 201808 and periodotx <201908 AND DSPTIPOTRX = 'Establecimientos'
group by A.IdenCuenta 
)
select distinct a.*, b.maxlinea_bench, flagscastigado, flagrefinanciado, flagpld, 
flagdiseftcexterno, b.FLGRCC , segmentoNSE,
ConsumoAnual201808_201908,NroTransaccionAnual201808_201908, tipo_seguro, 
isnull(c.[201812],-1) as [HabitoPago201812], isnull(c.[201901],-1) as [HabitoPago201901],isnull(c.[201902], -1) as [HabitoPago201902], 
isnull(c.[201903], -1) as [HabitoPago201903], isnull(c.[201906],-1) as [HabitoPago201906], isnull(c.[201907],-1) as [HabitoPago201907],---mod here
isnull(c.[201904],-1) as [HabitoPago201904],isnull(c.[201905],-1) as [HabitoPago201905], isnull(c.[201908],-1) as [HabitoPago201908],
isnull(c.[201811],-1) as [HabitoPago201811], isnull(c.[201810],-1) as [HabitoPago201810]
--habitoP1, habitop2, habitop3,
--habitoP4, habitop5, habitop6,
 --CASE WHEN HABITOP1 IN (1,0) OR  HABITOP2 IN (1,0) OR HABITOP3 IN (1,0) THEN '0' else  '1'	END [FlagHabitoPago3M],
-- CASE WHEN  HABITOP1 IN (1,0) OR  HABITOP2 IN (1,0) OR HABITOP3 IN (1,0) 
	--or  HABITOP4 IN (1,0) OR  HABITOP5 IN (1,0) OR HABITOP6 IN (1,0)  THEN '0' else  '1' END [FlagHabitoPago6M]
INTO ModeloCliente201908
From tmptotal201908 a left join tmprcc b on (a.numdoc = b.nrodocumento)
	left join tmpConsumo E on (a.idencuenta = E.idencuenta)
	left join base_susalud d on (a.numdoc = d.num_doc)
	LEFT JOIN VHABITOPAGO C ON (a.idencuenta = c.idencuenta)
where flg_mayor = 1 and diasmora201908 < 8


--modificar 3 meses y 6 meses
with tmpMora3m as(
select distinct id_cuenta, dias_atraso From [OR_TMP_REPORT_PROV_2_2]
where periodo =201911
), tmpMora6m as(
select distinct id_cuenta, dias_atraso From [OR_TMP_REPORT_PROV_2_2]
where periodo =202002
), tmpFLAG as 
(
select a.*, b.dias_Atraso as DiasAtraso3m, c.dias_atraso as DiasAtraso6m 
 From (ModeloCliente201908 a left join tmpMora3m b on a.idencuenta = b.id_cuenta)
	left join tmpMora6m c on  a.idencuenta = c.id_cuenta
WHERE idencuenta not in 
(select idencuenta 
 FRom ModeloCliente201908
 where HabitoPago201905 = -1  and HabitoPago201904 = -1
 and HabitoPago201908 = -1 and HabitoPago201907 = -1 and HabitoPago201906 =-1
 and HabitoPago201903 = -1  )
 ) SELECT *, case when diasatraso3m > 8 or diasAtraso6m > 8 then 'MAL CLIENTE'
ELSE 'BUEN CLIENTE' END [FlagCliente] FROM TMPflag 
