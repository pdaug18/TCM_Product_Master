-- ============================================================
-- VALIDATION: ALL_SALES_HIST vs ALL_SALES_HIST_TEST
--   Compares sales (Total Price) and units (Unit Quantity)
--   by vertical and year across both tables.

--   DYNAMIC TABLES: Ensure these are refreshed before running validation.
--   PROD: BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
--   TEST: BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST
--
--   - Year derived from YEAR("CALENDAR DATE")
--   - Delta = TEST value minus PROD value
--     (positive = TEST has more, negative = TEST has less)
--   - Sales MATCH uses ROUND(..., 2) to avoid float drift
-- ============================================================


-- ============================================================
-- QUERY 0: ROW COUNT BY YEAR
--   Baseline check: does each table have the same number
--   of rows per year before aggregating?
-- ============================================================
WITH prod AS (
    SELECT
        YEAR("CALENDAR DATE")  AS year,
        COUNT(*)               AS prod_row_count
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
    GROUP BY YEAR("CALENDAR DATE")
),
test AS (
    SELECT
        YEAR("CALENDAR DATE")  AS year,
        COUNT(*)               AS test_row_count
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST
    GROUP BY YEAR("CALENDAR DATE")
)
SELECT
    COALESCE(p.year, t.year)                                          AS year,
    COALESCE(p.prod_row_count, 0)                                     AS prod_row_count,
    COALESCE(t.test_row_count, 0)                                     AS test_row_count,
    COALESCE(t.test_row_count, 0) - COALESCE(p.prod_row_count, 0)    AS delta_count,
    CASE
        WHEN COALESCE(t.test_row_count, 0) = COALESCE(p.prod_row_count, 0)
            THEN 'MATCH'
        ELSE 'MISMATCH'
    END                                                               AS status
FROM prod   p
FULL OUTER JOIN test t ON p.year = t.year
ORDER BY COALESCE(p.year, t.year);
/* RESULT:
┌─────────┬┬───────────────┬───────────────┬─────────────┬────────┐
│ YEAR    ││ PROD_ROW_COUNT│ TEST_ROW_COUNT│ DELTA_COUNT │ STATUS │
├─────────┼┼───────────────┼───────────────┼─────────────┼────────┤
│ 2018    ││ 1015          │ 1015          │ 0           │ MATCH  │
│ 2019    ││ 163555        │ 163555        │ 0           │ MATCH  │
│ 2020    ││ 284362        │ 284362        │ 0           │ MATCH  │
│ 2021    ││ 355129        │ 355129        │ 0           │ MATCH  │
│ 2022    ││ 364452        │ 364452        │ 0           │ MATCH  │
│ 2023    ││ 410355        │ 410355        │ 0           │ MATCH  │
│ 2024    ││ 439732        │ 439732        │ 0           │ MATCH  │
│ 2025    ││ 496620        │ 496620        │ 0           │ MATCH  │
│ 2026    ││ 136471        │ 136471        │ 0           │ MATCH  │
└─────────┴┴───────────────┴───────────────┴─────────────┴────────┘

*/


-- ============================================================
-- QUERY 1: SALES & UNITS BY "VERTICAL (Calc)" + YEAR
-- ============================================================
WITH prod AS (
    SELECT
        "VERTICAL (Calc)"              AS vertical_calc,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS prod_sales,
        SUM("Unit Quantity")           AS prod_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
    GROUP BY "VERTICAL (Calc)", YEAR("CALENDAR DATE")
),
test AS (
    SELECT
        "VERTICAL (Calc)"              AS vertical_calc,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS test_sales,
        SUM("Unit Quantity")           AS test_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST
    GROUP BY "VERTICAL (Calc)", YEAR("CALENDAR DATE")
)
SELECT
    COALESCE(p.vertical_calc, t.vertical_calc)                                AS vertical_calc,
    COALESCE(p.year, t.year)                                                   AS year,
    COALESCE(p.prod_sales, 0)                                                  AS prod_sales,
    COALESCE(t.test_sales, 0)                                                  AS test_sales,
    COALESCE(t.test_sales, 0) - COALESCE(p.prod_sales, 0)                     AS delta_sales,
    COALESCE(p.prod_units, 0)                                                  AS prod_units,
    COALESCE(t.test_units, 0)                                                  AS test_units,
    COALESCE(t.test_units, 0) - COALESCE(p.prod_units, 0)                     AS delta_units,
    CASE
        WHEN ROUND(COALESCE(t.test_sales, 0), 2) = ROUND(COALESCE(p.prod_sales, 0), 2)
         AND COALESCE(t.test_units, 0) = COALESCE(p.prod_units, 0)
            THEN 'MATCH'
        ELSE 'MISMATCH'
    END                                                                        AS status
FROM prod   p
FULL OUTER JOIN test t
    ON  p.vertical_calc = t.vertical_calc
    AND p.year          = t.year
ORDER BY
    COALESCE(p.vertical_calc, t.vertical_calc),
    COALESCE(p.year, t.year);
/* RESULT: 
┌─────────────────┬──────┬──────────────┬──────────────┬─────────────┬────────────┬────────────┬──────────────┬──────────┐
│ VERTICAL_CALC   │ YEAR │ PROD_SALES   │ TEST_SALES   │ DELTA_SALES │ PROD_UNITS │ TEST_UNITS │ DELTA_UNITS  │ STATUS   │
├─────────────────┼──────┼──────────────┼──────────────┼─────────────┼────────────┼────────────┼──────────────┼──────────┤
│ INDUSTRIAL PPE  │ 2018 │ 2115389.09   │ 2115389.09   │ 0           │ 2098251    │ 2098251    │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2019 │ 83975844.21  │ 83975844.21  │ 0           │ 10060557   │ 10060557   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2020 │ 170607419.79 │ 170607419.79 │ 0           │ 21905740   │ 21905740   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2021 │ 188185819.83 │ 188185819.83 │ 0           │ 16854036   │ 16854036   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2022 │ 245878348.57 │ 245878348.57 │ 0           │ 18456013   │ 18456013   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2023 │ 294007471.31 │ 294007471.31 │ 0           │ 49391205   │ 49391205   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2024 │ 324174162.83 │ 324174162.83 │ 0           │ 20045166   │ 20045166   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2025 │ 346046509.32 │ 346046509.32 │ 0           │ 14307423   │ 14307423   │ 0            │ MATCH    │
│ INDUSTRIAL PPE  │ 2026 │ 83392567.44  │ 83392567.44  │ 0           │ 2189346    │ 2189346    │ 0            │ MATCH    │
│ null            │ 2018 │ 4589.48      │ 0            │ -4589.48    │ 88         │ 0          │ -88          │ MISMATCH │
│ null            │ 2018 │ 0            │ 4589.48      │ 4589.48     │ 0          │ 88         │ 88           │ MISMATCH │
│ null            │ 2019 │ 0            │ 372.68       │ 372.68      │ 0          │ -75        │ -75          │ MISMATCH │
│ null            │ 2019 │ 372.68       │ 0            │ -372.68     │ -75        │ 0          │ 75           │ MISMATCH │
│ null            │ 2020 │ -12794.78    │ 0            │ 12794.78    │ -91        │ 0          │ 91           │ MISMATCH │
│ null            │ 2020 │ 0            │ -12794.78    │ -12794.78   │ 0          │ -91        │ -91          │ MISMATCH │
│ null            │ 2021 │ 0            │ 150.16       │ 150.16      │ 0          │ 0          │ 0            │ MISMATCH │
│ null            │ 2021 │ 150.16       │ 0            │ -150.16     │ 0          │ 0          │ 0            │ MISMATCH │
│ null            │ 2022 │ 3111.91      │ 0            │ -3111.91    │ 132        │ 0          │ -132         │ MISMATCH │
│ null            │ 2022 │ 0            │ 3111.91      │ 3111.91     │ 0          │ 132        │ 132          │ MISMATCH │
│ null            │ 2023 │ 0            │ 13886.02     │ 13886.02    │ 0          │ -28        │ -28          │ MISMATCH │
│ null            │ 2023 │ 13886.02     │ 0            │ -13886.02   │ -28        │ 0          │ 28           │ MISMATCH │
│ null            │ 2024 │ 0            │ -8902.69     │ -8902.69    │ 0          │ 17         │ 17           │ MISMATCH │
│ null            │ 2024 │ -8902.69     │ 0            │ 8902.69     │ 17         │ 0          │ -17          │ MISMATCH │
│ null            │ 2025 │ 0            │ 18027.85     │ 18027.85    │ 0          │ 77         │ 77           │ MISMATCH │
│ null            │ 2025 │ 18027.85     │ 0            │ -18027.85   │ 77         │ 0          │ -77          │ MISMATCH │
│ null            │ 2026 │ 0            │ -12436.75    │ -12436.75   │ 0          │ -70        │ -70          │ MISMATCH │
│ null            │ 2026 │ -12436.75    │ 0            │ 12436.75    │ -70        │ 0          │ 70           │ MISMATCH │
└─────────────────┴──────┴──────────────┴──────────────┴─────────────┴────────────┴────────────┴──────────────┴──────────┘

*/

-- ============================================================
-- QUERY 2: SALES & UNITS BY "PRODUCT CATEGORY/VERTICAL" + YEAR
-- ============================================================
WITH prod AS (
    SELECT
        "PRODUCT CATEGORY/VERTICAL"    AS product_category_vertical,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS prod_sales,
        SUM("Unit Quantity")           AS prod_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
    GROUP BY "PRODUCT CATEGORY/VERTICAL", YEAR("CALENDAR DATE")
),
test AS (
    SELECT
        "PRODUCT CATEGORY/VERTICAL"    AS product_category_vertical,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS test_sales,
        SUM("Unit Quantity")           AS test_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST
    GROUP BY "PRODUCT CATEGORY/VERTICAL", YEAR("CALENDAR DATE")
)
SELECT
    COALESCE(p.product_category_vertical, t.product_category_vertical)         AS product_category_vertical,
    COALESCE(p.year, t.year)                                                    AS year,
    COALESCE(p.prod_sales, 0)                                                   AS prod_sales,
    COALESCE(t.test_sales, 0)                                                   AS test_sales,
    COALESCE(t.test_sales, 0) - COALESCE(p.prod_sales, 0)                      AS delta_sales,
    COALESCE(p.prod_units, 0)                                                   AS prod_units,
    COALESCE(t.test_units, 0)                                                   AS test_units,
    COALESCE(t.test_units, 0) - COALESCE(p.prod_units, 0)                      AS delta_units,
    CASE
        WHEN ROUND(COALESCE(t.test_sales, 0), 2) = ROUND(COALESCE(p.prod_sales, 0), 2)
         AND COALESCE(t.test_units, 0) = COALESCE(p.prod_units, 0)
            THEN 'MATCH'
        ELSE 'MISMATCH'
    END                                                                         AS status
FROM prod   p
FULL OUTER JOIN test t
    ON  p.product_category_vertical = t.product_category_vertical
    AND p.year                      = t.year
ORDER BY
    COALESCE(p.product_category_vertical, t.product_category_vertical),
    COALESCE(p.year, t.year);
/* RESULT:
PRODUCT_CATEGORY_VERTICAL	YEAR	PROD_SALES	TEST_SALES	DELTA_SALES	PROD_UNITS	TEST_UNITS	DELTA_UNITS	STATUS
05	                        2018	379493.74	379493.74	0	        4632	    4632	    0	        MATCH
05	                        2019	36899075.38	36899075.38	0	        597655	    597655	    0	        MATCH
05	                        2020	66301349.92	66301349.92	0	        2125097	    2125097	    0	        MATCH
05	                        2021	76407115.93	76407115.93	0	        1550655	    1550655	    0	        MATCH
05	                        2022	83739993.99	83739993.99	0	        1242515	    1242515	    0	        MATCH
05	                        2023	82928494.54	82928494.54	0	        1214762	    1214762	    0	        MATCH
05	                        2024	88940673.76	88940673.76	0	        1222174	    1222174	    0	        MATCH
05	                        2025	100872938.48	100872938.48	0	1472684	    1472684	    0	        MATCH
05	                        2026	23444415.63	23444415.63	0	        347192	    347192	    0	        MATCH
06	                        2018	211895.5	211895.5	0	        432538	    432538	    0	        MATCH
06	                        2019	1902354.73	1902354.73	0	        2657154	    2657154	    0	        MATCH
06	                        2020	3850779.92	3850779.92	0	        3921242	    3921242	    0	        MATCH
06	                        2021	2682502.54	2682502.54	0	        3947342	    3947342	    0	        MATCH
06	                        2022	5213955.73	5213955.73	0	        3768349	    3768349	    0	        MATCH
06	                        2023	4673734.54	4673734.54	0	        6153086	    6153086	    0	        MATCH
06	                        2024	14461611.5	14461611.5	0	        8917964	    8917964	    0	        MATCH
06	                        2025	3155318.24	3155318.24	0	        2035700	    2035700	    0	        MATCH
06	                        2026	234003.94	234003.94	0	        151306	    151306	    0	        MATCH
10	                        2018	46726.66	46726.66	0	        824	        824	        0	        MATCH
10	                        2019	9916538.33	9916538.33	0	        187473	    187473	    0	        MATCH
10	                        2020	19283134.8	19283134.8	0	        391105	    391105	    0	        MATCH
10	                        2021	25681015.77	25681015.77	0	        480050	    480050	    0	        MATCH
10	                        2022	43499428.79	43499428.79	0	        580551	    580551	    0	        MATCH
10	                        2023	62155063.52	62155063.52	0	        664358	    664358	    0	        MATCH
10	                        2024	78011800.24	78011800.24	0	        738862	    738862	    0	        MATCH
10	                        2025	84843121.01	84843121.01	0	        892075	    892075	    0	        MATCH
10	                        2026	21914306.28	21914306.28	0	        245565	    245565	    0	        MATCH
11	                        2022	6834161.69	6834161.69	0	        232191	    232191	    0	        MATCH
11	                        2023	9430794.39	9430794.39	0	        366097	    366097	    0	        MATCH
11	                        2024	7727226.78	7727226.78	0	        265182	    265182	    0	        MATCH
11	                        2025	6513014.89	6513014.89	0	        195031	    195031	    0	        MATCH
11	                        2026	1405060.73	1405060.73	0	        43301	    43301	    0	        MATCH
15	                        2018	227383.95	227383.95	0	        4285	    4285	    0	        MATCH
15	                        2019	12612620.37	12612620.37	0	        344542	    344542	    0	        MATCH
15	                        2020	20335746.79	20335746.79	0	        580744	    580744	    0	        MATCH
15	                        2021	26922677.34	26922677.34	0	        680067	    680067	    0	        MATCH
15	                        2022	30273852.56	30273852.56	0	        613023	    613023	    0	        MATCH
15	                        2023	33061166.41	33061166.41	0	        630616	    630616	    0	        MATCH
15	                        2024	34782615.64	34782615.64	0	        604831	    604831	    0	        MATCH
15	                        2025	41110631.96	41110631.96	0	        742831	    742831	    0	        MATCH
15	                        2026	9771009.08	9771009.08	0	        177691	    177691	    0	        MATCH
20	                        2018	9599.4	    9599.4	0	            446	        446	        0	        MATCH
20	                        2019	4027095.62	4027095.62	0	        142028	    142028	    0	        MATCH
20	                        2020	16663194.87	16663194.87	0	        1465562	    1465562	    0	        MATCH
20	                        2021	9927328.64	9927328.64	0	        322075	    322075	    0	        MATCH
20	                        2022	9566041.93	9566041.93	0	        231389	    231389	    0	        MATCH
20	                        2023	9678210.8	9678210.8	0	        190082	    190082	    0	        MATCH
20	                        2024	10370782.22	10370782.22	0	        546821	    546821	    0	        MATCH
20	                        2025	16845950.22	16845950.22	0	        2643175	    2643175	    0	        MATCH
20	                        2026	3357917.79	3357917.79	0	        480889	    480889	    0	        MATCH
25	                        2018	164064.27	164064.27	0	        4629	    4629	    0	        MATCH
25	                        2019	12390876.33	12390876.33	0	        154055	    154055	    0	        MATCH
25	                        2020	22497189.38	22497189.38	0	        288468	    288468	    0	        MATCH
25	                        2021	21767996.2	21767996.2	0	        212121	    212121	    0	        MATCH
25	                        2022	28457930.92	28457930.92	0	        192201	    192201	    0	        MATCH
25	                        2023	35214587.61	35214587.61	0	        234603	    234603	    0	        MATCH
25	                        2024	41428581.67	41428581.67	0	        247477	    247477	    0	        MATCH
25	                        2025	45296747.22	45296747.22	0	        252738	    252738	    0	        MATCH
25	                        2026	6302134.47	6302134.47	0	        55432	    55432	    0	        MATCH
26	                        2019	3024059.09	3024059.09	0	        4197830	    4197830	    0	        MATCH
26	                        2020	8787397.31	8787397.31	0	        12144881	12144881	0	        MATCH
26	                        2021	4664917.18	4664917.18	0	        8521361	    8521361	    0	        MATCH
26	                        2022	5675388.92	5675388.92	0	        9058573	    9058573	    0	        MATCH
26	                        2023	12296670.32	12296670.32	0	        36237675	36237675	0	        MATCH
26	                        2024	6326051.92	6326051.92	0	        5551855	    5551855	    0	        MATCH
26	                        2025	5889448.65	5889448.65	0	        4875166	    4875166	    0	        MATCH
26	                        2026	609867.1	609867.1	0	        295723	    295723	    0	        MATCH
30	                        2020	4644868.77	4644868.77	0	        142724	    142724	    0	        MATCH
30	                        2021	11875058.42	11875058.42	0	        405963	    405963	    0	        MATCH
30	                        2022	23714088.8	23714088.8	0	        870058	    870058	    0	        MATCH
30	                        2023	29081289.68	29081289.68	0	        1047726	    1047726	    0	        MATCH
30	                        2024	24376816.57	24376816.57	0	        788845	    788845	    0	        MATCH
30	                        2025	28332150.92	28332150.92	0	        754057	    754057	    0	        MATCH
30	                        2026	7472511.29	7472511.29	0	        191588	    191588	    0	        MATCH
35	                        2019	259490.3	259490.3	0	        3295	    3295	    0	        MATCH
35	                        2020	2721488	    2721488	    0	        264697	    264697	    0	        MATCH
35	                        2021	4286152.46	4286152.46	0	        401900	    401900	    0	        MATCH
35	                        2022	5023308.73	5023308.73	0	        244468	    244468	    0	        MATCH
35	                        2023	4279659.02	4279659.02	0	        208625	    208625	    0	        MATCH
35	                        2024	5776774.75	5776774.75	0	        215953	    215953	    0	        MATCH
35	                        2025	3427624.3	3427624.3	0	        111344	    111344	    0	        MATCH
35	                        2026	2324467.34	2324467.34	0	        88391	    88391	    0	        MATCH
40	                        2020	613162.59	613162.59	0	        14163	    14163	    0	        MATCH
40	                        2021	707778.66	707778.66	0	        16513	    16513	    0	        MATCH
40	                        2022	774937.46	774937.46	0	        13381	    13381	    0	        MATCH
40	                        2023	919436.12	919436.12	0	        10042	    10042	    0	        MATCH
40	                        2024	1502378.04	1502378.04	0	        15568	    15568	    0	        MATCH
40	                        2025	1265861.05	1265861.05	0	        10323	    10323	    0	        MATCH
40	                        2026	370622.28	370622.28	0	        3131	    3131	    0	        MATCH
45	                        2018	6625	    6625	    0	        25	        25	        0	        MATCH
45	                        2019	0	        0	        0	        0	        0	        0	        MATCH
45	                        2020	541092.41	541092.41	0	        23816	    23816	    0	        MATCH
45	                        2021	2611195.57	2611195.57	0	        103746	    103746	    0	        MATCH
45	                        2022	3014525.61	3014525.61	0	        79797	    79797	    0	        MATCH
45	                        2023	10186675.49	10186675.49	0	        648534	    648534	    0	        MATCH
45	                        2024	10376107.5	10376107.5	0	        763238	    763238	    0	        MATCH
45	                        2025	4339556.46	4339556.46	0	        150316	    150316	    0	        MATCH
45	                        2026	776475.06	776475.06	0	        14871	    14871	    0	        MATCH
50	                        2025	4074258.64	4074258.64	0	        42471	    42471	    0	        MATCH
50	                        2026	5368550.9	5368550.9	0	        82721	    82721	    0	        MATCH
60	                        2018	1039537.5	1039537.5	0	        1650551	    1650551	    0	        MATCH
60	                        2019	63141.99	63141.99	0	        1710572	    1710572	    0	        MATCH
60	                        2020	46823.24	46823.24	0	        275009	    275009	    0	        MATCH
60	                        2021	58903.82	58903.82	0	        203661	    203661	    0	        MATCH
60	                        2022	71388.24	71388.24	0	        1326254	    1326254	    0	        MATCH
60	                        2023	64174.76	64174.76	0	        1784503	    1784503	    0	        MATCH
60	                        2024	55172.09	55172.09	0	        165927	    165927	    0	        MATCH
60	                        2025	24077.25	24077.25	0	        128800	    128800	    0	        MATCH
60	                        2026	25423.15	25423.15	0	        11183	    11183	    0	        MATCH
98	                        2022	6699.44	    6699.44	    0	        81	        81	        0	        MATCH
98	                        2023	6497.97	    6497.97	    0	        97	        97	        0	        MATCH
98	                        2024	16113.28	16113.28	0	        204	        204	        0	        MATCH
98	                        2025	28851.53	28851.53	0	        422	        422	        0	        MATCH
98	                        2026	9864.13	    9864.13	    0	        333	        333	        0	        MATCH
99	                        2019	-326.13	    -326.13	    0	        -3	        -3	        0	        MATCH
99	                        2020	25543.52	25543.52	0	        226	        226	        0	        MATCH
99	                        2021	27312.87	27312.87	0	        330	        330	        0	        MATCH
99	                        2022	15217.42	15217.42	0	        3195	    3195	    0	        MATCH
99	                        2023	31016.14	31016.14	0	        399	        399	        0	        MATCH
99	                        2024	21539.65	21539.65	0	        266	        266	        0	        MATCH
99	                        2025	26958.5	    26958.5	    0	        290	        290	        0	        MATCH
99	                        2026	5938.27	    5938.27	    0	        29	        29	        0	        MATCH
C	                        2018	7383.69	    7383.69	    0	        116	        116	        0	        MATCH
C	                        2019	1817477.14	1817477.14	0	        22287	    22287	    0	        MATCH
C	                        2020	1900646.71	1900646.71	0	        22401	    22401	    0	        MATCH
C	                        2021	398545.38	398545.38	0	        2465	    2465	    0	        MATCH
C	                        2022	-2571.66	-2571.66	0	        -13	        -13	        0	        MATCH
C	                        2024	0	        0	        0	        0	        0	        0	        MATCH
C	                        2025	0	        0	        0	        0	        0	        0	        MATCH
D1	                        2018	13750.48	13750.48	0	        76	        76	        0	        MATCH
D1	                        2019	29851.24	29851.24	0	        354	        354	        0	        MATCH
D1	                        2020	18523.36	18523.36	0	        228	        228	        0	        MATCH
D1	                        2021	4843.64	    4843.64	    0	        24	        24	        0	        MATCH
DG	                        2018	2025.11	    2025.11	    0	        62	        62	        0	        MATCH
DG	                        2019	465441.83	465441.83	0	        9991	    9991	    0	        MATCH
DG	                        2020	146671.46	146671.46	0	        4182	    4182	    0	        MATCH
DG	                        2021	131191.74	131191.74	0	        3464	    3464	    0	        MATCH
DG	                        2022	0	        0	        0	        0	        0	        0	        MATCH
DK	                        2021	0	        0	        0	        0	        0	        0	        MATCH
DT	                        2021	0	        0	        0	        0	        0	        0	        MATCH
DZ	                        2019	-17.16	    -17.16	    0	        11	        11	        0	        MATCH
DZ	                        2020	0	        0	        0	        10	        10	        0	        MATCH
FG	                        2019	5070.88	    5070.88	    0	        330	        330	        0	        MATCH
H	                        2018	82.12	    82.12	    0	        4	        4	        0	        MATCH
H	                        2019	43730.32	43730.32	0	        1903	    1903	    0	        MATCH
H	                        2020	36846.24	36846.24	0	        1774	    1774	    0	        MATCH
H	                        2021	18986.33	18986.33	0	        863	        863	        0	        MATCH
HC	                        2020	1541011.04	1541011.04	0	        193222	    193222	    0	        MATCH
HC	                        2021	5137.2	    5137.2	    0	        1206	    1206	    0	        MATCH
HK	                        2018	70.2	    70.2	    0	        2	        2	        0	        MATCH
HK	                        2019	463152.17	463152.17	0	        29864	    29864	    0	        MATCH
HK	                        2020	623825.93	623825.93	0	        45669	    45669	    0	        MATCH
HK	                        2021	1820.52	    1820.52	    0	        132	        132	        0	        MATCH
J	                        2019	10448.21	10448.21	0	        201	        201	        0	        MATCH
J	                        2021	0	        0	        0	        0	        0	        0	        MATCH
L	                        2019	246.45	    246.45	    0	        3	        3	        0	        MATCH
L	                        2020	3144.86	    3144.86	    0	        38	        38	        0	        MATCH
L	                        2021	985.2	    985.2	    0	        12	        12	        0	        MATCH
M	                        2018	851.28	    851.28	    0	        24	        24	        0	        MATCH
M	                        2019	25135.08	25135.08	0	        684	        684	        0	        MATCH
M	                        2020	6194.16	    6194.16	    0	        168	        168	        0	        MATCH
M	                        2022	0	        0	        0	        0	        0	        0	        MATCH
O	                        2020	-4182.46	-4182.46	0	        -4	        -4	        0	        MATCH
O	                        2024	-82.78	    -82.78	    0	        -1	        -1	        0	        MATCH
S	                        2018	0	        0	        0	        0	        0	        0	        MATCH
S	                        2019	1813	    1813	    0	        74	        74	        0	        MATCH
S	                        2020	18843.96	18843.96	0	        244	        244	        0	        MATCH
S	                        2021	2368.84	    2368.84	    0	        72	        72	        0	        MATCH
T	                        2018	5900.19	    5900.19	    0	        37	        37	        0	        MATCH
T	                        2019	18569.04	18569.04	0	        254	        254	        0	        MATCH
T	                        2020	4123.01	    4123.01	    0	        74	        74	        0	        MATCH
T	                        2021	1985.58	    1985.58	    0	        14	        14	        0	        MATCH
T	                        2022	0	        0	        0	        0	        0	        0	        MATCH
T	                        2026	0	        0	        0	        0	        0	        0	        MATCH

*/

-- ============================================================
-- QUERY 3: SALES & UNITS BY "TCM Historical Vertical" + YEAR
-- ============================================================
WITH prod AS (
    SELECT
        "TCM Historical Vertical"      AS tcm_historical_vertical,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS prod_sales,
        SUM("Unit Quantity")           AS prod_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST
    GROUP BY "TCM Historical Vertical", YEAR("CALENDAR DATE")
),
test AS (
    SELECT
        "TCM Historical Vertical"      AS tcm_historical_vertical,
        YEAR("CALENDAR DATE")          AS year,
        SUM("Total Price")             AS test_sales,
        SUM("Unit Quantity")           AS test_units
    FROM BRONZE_DATA.TCM_BRONZE.ALL_SALES_HIST_TEST
    GROUP BY "TCM Historical Vertical", YEAR("CALENDAR DATE")
)
SELECT
    COALESCE(p.tcm_historical_vertical, t.tcm_historical_vertical)             AS tcm_historical_vertical,
    COALESCE(p.year, t.year)                                                    AS year,
    COALESCE(p.prod_sales, 0)                                                   AS prod_sales,
    COALESCE(t.test_sales, 0)                                                   AS test_sales,
    COALESCE(t.test_sales, 0) - COALESCE(p.prod_sales, 0)                      AS delta_sales,
    COALESCE(p.prod_units, 0)                                                   AS prod_units,
    COALESCE(t.test_units, 0)                                                   AS test_units,
    COALESCE(t.test_units, 0) - COALESCE(p.prod_units, 0)                      AS delta_units,
    CASE
        WHEN ROUND(COALESCE(t.test_sales, 0), 2) = ROUND(COALESCE(p.prod_sales, 0), 2)
         AND COALESCE(t.test_units, 0) = COALESCE(p.prod_units, 0)
            THEN 'MATCH'
        ELSE 'MISMATCH'
    END                                                                         AS status
FROM prod   p
FULL OUTER JOIN test t
    ON  p.tcm_historical_vertical = t.tcm_historical_vertical
    AND p.year                    = t.year
ORDER BY
    COALESCE(p.tcm_historical_vertical, t.tcm_historical_vertical),
    COALESCE(p.year, t.year);

/* Result:
TCM_HISTORICAL_VERTICAL	YEAR	PROD_SALES	TEST_SALES	DELTA_SALES	PROD_UNITS	TEST_UNITS	DELTA_UNITS	STATUS
	                    2018	4589.48	    4589.48	    0	        88	        88	        0	        MATCH
	                    2019	23.39	    23.39	    0	        -66	        -66	        0	        MATCH
	                    2020	-10354.07	-10354.07	0	        -76	        -76	        0	        MATCH
                        2021	150.16	    150.16	    0	        0	        0	        0	        MATCH
	                    2022	3111.91	    3111.91	    0	        132	        132	        0	        MATCH
	                    2023	13960.87	13960.87	0	        19	        19	        0	        MATCH
	                    2024	-10456.77	-10456.77	0	        11	        11	        0	        MATCH
	                    2025	12375.19	12375.19	0	        61	        61	        0	        MATCH
	                    2026	-12436.75	-12436.75	0	        -70	        -70	        0	        MATCH
05	                    2018	16072.83	16072.83	0	        325	        325	        0	        MATCH
05	                    2019	6285978.38	6285978.38	0	        133817	    133817	    0	        MATCH
05	                    2020	6267163.36	6267163.36	0	        153689	    153689	    0	        MATCH
05	                    2021	47498638.25	47498638.25	0	        965554	    965554	    0	        MATCH
05	                    2022	53138133.07	53138133.07	0	        798903	    798903	    0	        MATCH
05	                    2023	85548145.69	85548145.69	0	        1318513	    1318513	    0	        MATCH
05	                    2024	88447873.86	88447873.86	0	        1185304	    1185304	    0	        MATCH
05	                    2025	100848687.03	100848687.03	0	1437269	    1437269	    0	        MATCH
05	                    2026	23590636.41	23590636.41	0	        347602	    347602	    0	        MATCH
06	                    2024	15123316.06	15123316.06	0	        11649776	11649776	0	        MATCH
06	                    2025	2760210.62	2760210.62	0	        1776163	    1776163	    0	        MATCH
06	                    2026	232820.17	232820.17	0	        151139	    151139	    0	        MATCH
10	                    2018	7281.15	7281.15	        0	        150	        150	        0	        MATCH
10	                    2019	2026482.67	2026482.67	0	        121370	    121370	    0	        MATCH
10	                    2020	2513062.37	2513062.37	0	        125316	    125316	    0	        MATCH
10	                    2021	15949397.99	15949397.99	0	        384476	    384476	    0	        MATCH
10	                    2022	26453843.41	26453843.41	0	        468474	    468474	    0	        MATCH
10	                    2023	59953490.1	59953490.1	0	        551079	    551079	    0	        MATCH
10	                    2024	78398675.87	78398675.87	0	        767645	    767645	    0	        MATCH
10	                    2025	86484523.11	86484523.11	0	        941598	    941598	    0	        MATCH
10	                    2026	21928539.51	21928539.51	0	        246192	    246192	    0	        MATCH
11	                    2022	6834161.69	6834161.69	0	        232191	    232191	    0	        MATCH
11	                    2023	9427743.75	9427743.75	0	        366025	    366025	    0	        MATCH
11	                    2024	7726879.74	7726879.74	0	        265176	    265176	    0	        MATCH
11	                    2025	6508148.17	6508148.17	0	        194911	    194911	    0	        MATCH
11	                    2026	1405060.73	1405060.73	0	        43301	    43301	    0	        MATCH
15	                    2018	4122.63	    4122.63	    0	        242	        242	        0	        MATCH
15	                    2019	1879709.38	1879709.38	0	        72446	    72446	    0	        MATCH
15	                    2020	1774816.16	1774816.16	0	        68671	    68671	    0	        MATCH
15	                    2021	15649142.58	15649142.58	0	        433700	    433700	    0	        MATCH
15	                    2022	18068113.23	18068113.23	0	        402473	    402473	    0	        MATCH
15	                    2023	32354359.53	32354359.53	0	        632897	    632897	    0	        MATCH
15	                    2024	34342410.07	34342410.07	0	        586879	    586879	    0	        MATCH
15	                    2025	37941128.69	37941128.69	0	        636258	    636258	    0	        MATCH
15	                    2026	9335121.13	9335121.13	0	        175119	    175119	    0	        MATCH
20	                    2019	370413.02	370413.02	0	        4780	    4780	    0	        MATCH
20	                    2020	1142186.23	1142186.23	0	        14385	    14385	    0	        MATCH
20	                    2021	6109273.68	6109273.68	0	        181575	    181575	    0	        MATCH
20	                    2022	5313662.98	5313662.98	0	        126196	    126196	    0	        MATCH
20	                    2023	9543375.11	9543375.11	0	        188465	    188465	    0	        MATCH
20	                    2024	10671324.89	10671324.89	0	        571522	    571522	    0	        MATCH
20	                    2025	18287512.39	18287512.39	0	        2733542	    2733542	    0	        MATCH
20	                    2026	3651284.4	3651284.4	0	        482455	    482455	    0	        MATCH
25	                    2018	378.82	    378.82	    0	        13	        13	        0	        MATCH
25	                    2019	195156.62	195156.62	0	        6644	    6644	    0	        MATCH
25	                    2020	146259.63	146259.63	0	        4691	    4691	    0	        MATCH
25	                    2021	12592734.04	12592734.04	0	        134828	    134828	    0	        MATCH
25	                    2022	13712105.94	13712105.94	0	        116047	    116047	    0	        MATCH
25	                    2023	41214808.3	41214808.3	0	        555285	    555285	    0	        MATCH
25	                    2024	46526154.53	46526154.53	0	        914592	    914592	    0	        MATCH
25	                    2025	46611102.1	46611102.1	0	        354375	    354375	    0	        MATCH
25	                    2026	6285950.05	6285950.05	0	        55400	    55400	    0	        MATCH
26	                    2024	5208373.12	5208373.12	0	        2524401	    2524401	    0	        MATCH
26	                    2025	5786296.65	5786296.65	0	        4845486	    4845486	    0	        MATCH
26	                    2026	609867.1	609867.1	0	        295723	    295723	    0	        MATCH
30	                    2020	464665.64	464665.64	0	        5751	    5751	    0   	    MATCH
30	                    2021	7854544.43	7854544.43	0	        242620	    242620	    0	        MATCH
30	                    2022	14054671.51	14054671.51	0	        491923	    491923	    0	        MATCH
30	                    2023	30918182.48	30918182.48	0	        1315715	    1315715	    0	        MATCH
30	                    2024	24376816.57	24376816.57	0	        788845	    788845	    0	        MATCH
30	                    2025	28331501.48	28331501.48	0	        754033	    754033	    0	        MATCH
30	                    2026	7472511.29	7472511.29	0	        191588	    191588	    0	        MATCH
35	                    2020	281493.66	281493.66	0	        20868	    20868	    0	        MATCH
35	                    2021	3041480.93	3041480.93	0	        294829	    294829	    0	        MATCH
35	                    2022	4051802.92	4051802.92	0	        241704	    241704	    0	        MATCH
35	                    2023	7264505.48	7264505.48	0	        282137	    282137	    0	        MATCH
35	                    2024	6690354.05	6690354.05	0	        255516	    255516	    0	        MATCH
35	                    2025	3529661.61	3529661.61	0	        113526	    113526	    0	        MATCH
35	                    2026	2317248.14	2317248.14	0	        87985	    87985	    0	        MATCH
40	                    2023	32815	    32815	    0	        138	        138	        0	        MATCH
40	                    2024	1026067.35	1026067.35	0	        20820	    20820	    0	        MATCH
40	                    2025	797114.79	797114.79	0	        2900	    2900	    0	        MATCH
40	                    2026	530716.22	530716.22	0	        3493	    3493	    0	        MATCH
45	                    2020	20344.6	    20344.6	    0	        3820	    3820	    0	        MATCH
45	                    2021	41735.76	41735.76	0	        7982	    7982	    0	        MATCH
45	                    2022	18733.66	18733.66	0	        2801	    2801	    0	        MATCH
45	                    2023	507474.96	507474.96	0	        2147	    2147	    0	        MATCH
45	                    2024	4841038.39	4841038.39	0	        51317	    51317   	0	        MATCH
45	                    2025	3347628.23	3347628.23	0	        53800	    53800	    0	        MATCH
45	                    2026	620195.88	620195.88	0	        14906	    14906	    0	        MATCH
50	                    2025	4074258.64	4074258.64	0	        42471	    42471	    0	        MATCH
50	                    2026	5368550.9	5368550.9	0	        82721	    82721	    0	        MATCH
60	                    2021	4327004.71	4327004.71	0	        298526	    298526  	0	        MATCH
60	                    2022	3899245.64	3899245.64	0	        297262	    297262  	0	        MATCH
60	                    2023	17159114.14	17159114.14	0	        44178009	44178009	0	        MATCH
60	                    2024	765667.7	765667.7	0	        463053	    463053  	0	        MATCH
60	                    2025	684123.12	684123.12	0	        420358	    420358	    0	        MATCH
60	                    2026	26606.92	26606.92	0	        11350	    11350	    0	        MATCH
98	                    2023	5588.07	    5588.07	    0	        74	        74	        0	        MATCH
98	                    2024	1010.51	    1010.51	    0	        31	        31	        0	        MATCH
98	                    2025	28851.53	28851.53	0	        422	        422	        0	        MATCH
98	                    2026	9864.13	    9864.13	    0	        333	        333	        0	        MATCH
99	                    2023	28556.68	28556.68	0	        330	        330	        0	        MATCH
99	                    2024	21539.65	21539.65	0	        266	        266	        0	        MATCH
99	                    2025	26958.5	    26958.5	    0	        290	        290	        0	        MATCH
99	                    2026	5938.27	    5938.27	    0	        29	        29	        0	        MATCH
A	                    2018	6479.86	    6479.86	    0	        156	        156	        0	        MATCH
A	                    2019	687784.57	687784.57	0	        21757	    21757	    0	        MATCH
A	                    2020	1128763.35	1128763.35	0	        32302	    32302	    0	        MATCH
A	                    2021	676725.81	676725.81	0	        19204	    19204	    0	        MATCH
A	                    2022	874819.08	874819.08	0	        20890	    20890	    0	        MATCH
A	                    2023	-2150.7	    -2150.7	    0	        -10	        -10	        0	        MATCH
B	                    2019	45201.13	45201.13	0	        77	        77	        0	        MATCH
B	                    2020	125431.54	125431.54	0	        3622	    3622	    0	        MATCH
B	                    2021	25956.8	    25956.8	    0	        62	        62	        0	        MATCH
B	                    2022	34440.07	34440.07	0	        94	        94	        0	        MATCH
B	                    2023	786.4	    786.4	    0	        4	        4	        0	        MATCH
C	                    2018	261175.19	261175.19	0	        1833	    1833	    0	        MATCH
C	                    2019	22881905.32	22881905.32	0	        252207	    252207	    0	        MATCH
C	                    2020	36095331.61	36095331.61	0	        907442	    907442	    0	        MATCH
C	                    2021	19462006.21	19462006.21	0	        217010	    217010	    0	        MATCH
C	                    2022	21920059.78	21920059.78	0	        190155	    190155	    0	        MATCH
C	                    2023	57262.84	57262.84	0	        242	        242	        0	        MATCH
C	                    2024	6887.77	    6887.77	    0	        21	        21	        0	        MATCH
C	                    2025	2600.82	    2600.82	    0	        27	        27	        0	        MATCH
C	                    2026	1583.68	    1583.68	    0	        7	        7	        0	        MATCH
D	                    2018	871.2	    871.2	    0	        48	        48	        0	        MATCH
D	                    2019	3161.4	    3161.4	    0	        168	        168	        0	        MATCH
D	                    2020	127068.18	127068.18	0	        86768	    86768	    0	        MATCH
D	                    2021	20461.7	    20461.7	    0	        150	        150	        0	        MATCH
D	                    2022	81959.82	81959.82	0	        71	        71	        0	        MATCH
D1	                    2018	268751.29	268751.29	0	        6618	    6618	    0	        MATCH
D1	                    2019	16917770.2	16917770.2	0	        233744	    233744	    0	        MATCH
D1	                    2020	30290152.82	30290152.82	0	        405927	    405927	    0	        MATCH
D1	                    2021	14374738.13	14374738.13	0	        151473	    151473	    0	        MATCH
D1	                    2022	19821974.92	19821974.92	0	        150461	    150461	    0	        MATCH
D1	                    2023	-12507.2	-12507.2	0	        18	        18	        0	        MATCH
D1	                    2024	921.78	    921.78	    0	        2	        2	        0	        MATCH
D2	                    2018	1230660	    1230660	    0	        2080800	    2080800	    0	        MATCH
D2	                    2019	4244941.51	4244941.51	0	        7898354	    7898354	    0	        MATCH
D2	                    2020	11815687.85	11815687.85	0	        15658532	15658532	0	        MATCH
D2	                    2021	2487012.95	2487012.95	0	        10607393	10607393	0	        MATCH
D2	                    2022	6501763.93	6501763.93	0	        12569349	12569349	0	        MATCH
DC	                    2021	640	        640	        0	        4	        4	        0	        MATCH
DE	                    2019	42.3	    42.3	    0	        2	        2	        0	        MATCH
DE	                    2022	37.9	    37.9	    0	        17	        17	        0	        MATCH
DF	                    2019	478	        478	        0	        100	        100	        0	        MATCH
DF	                    2020	5387.72	    5387.72	    0	        1443	    1443	    0	        MATCH
DG	                    2018	56718.21	56718.21	0	        2076	    2076	    0	        MATCH
DG	                    2019	1227564.96	1227564.96	0	        40290	    40290	    0	        MATCH
DG	                    2020	2215427.02	2215427.02	0	        69684	    69684	    0	        MATCH
DG	                    2021	1085185.47	1085185.47	0	        36244	    36244	    0	        MATCH
DG	                    2022	1390362.88	1390362.88	0	        39611	    39611	    0	        MATCH
DG	                    2023	-35.48	    -35.48	    0	        1	        1	        0	        MATCH
DH	                    2018	47.28	    47.28	    0	        3	        3	        0	        MATCH
DH	                    2019	23494.35	23494.35	0	        1388	    1388	    0	        MATCH
DH	                    2020	41294.42	41294.42	0	        2362	    2362	    0	        MATCH
DH	                    2021	44079.35	44079.35	0	        2343	    2343	    0	        MATCH
DH	                    2022	54834.28	54834.28	0	        2671	    2671	    0	        MATCH
DJ	                    2019	18134.84	18134.84	0	        61	        61	        0	        MATCH
DJ	                    2020	18102	    18102	    0	        60	        60	        0	        MATCH
DJ	                    2021	3765.54	    3765.54	    0	        12	        12	        0	        MATCH
DJ	                    2022	2962.19	    2962.19	    0	        8	        8	        0	        MATCH
DK	                    2021	0	        0	        0	        0	        0	        0	        MATCH
DS	                    2019	879	        879	        0	        60	        60	        0	        MATCH
DS	                    2021	944.7	    944.7	    0	        200	        200	        0	        MATCH
DS	                    2022	1254.8	    1254.8	    0	        79	        79	        0	        MATCH
DT	                    2021	0	        0	        0	        0	        0	        0	        MATCH
DZ	                    2019	86236.66	86236.66	0	        4227	    4227	    0	        MATCH
DZ	                    2020	259882.4	259882.4	0	        14832	    14832	    0	        MATCH
DZ	                    2021	169444.04	169444.04	0	        10204	    10204	    0	        MATCH
DZ	                    2022	213107.48	213107.48	0	        10606	    10606	    0	        MATCH
DZ	                    2023	-14.95	    -14.95	    0	        0	        0	        0	        MATCH
E	                    2018	14547.83	14547.83	0	        35	        35	        0	        MATCH
E	                    2019	4608959.79	4608959.79	0	        18809	    18809	    0	        MATCH
E	                    2020	7691824.31	7691824.31	0	        30128	    30128	    0	        MATCH
E	                    2021	6355464.69	6355464.69	0	        22981	    22981	    0	        MATCH
E	                    2022	11674761.16	11674761.16	0	        31546	    31546	    0	        MATCH
E	                    2023	2445.35	    2445.35	    0	        4	        4	        0	        MATCH
FG	                    2020	58528.97	58528.97	0	        1809	    1809	    0	        MATCH
FG	                    2021	21620.44	21620.44	0	        413	        413	        0	        MATCH
FG	                    2022	5971831.94	5971831.94	0	        247351	    247351	    0	        MATCH
FG	                    2023	191.6	    191.6	    0	        1	        1	        0	        MATCH
G	                    2018	14058.93	14058.93	0	        560	        560 	    0	        MATCH
G	                    2019	1798303.6	1798303.6	0	        41325	    41325	    0	        MATCH
G	                    2020	4716191.88	4716191.88	0	        91217	    91217	    0	        MATCH
G	                    2021	1810368.07	1810368.07	0	        36790	    36790	    0	        MATCH
G	                    2022	1721558.22	1721558.22	0	        31784	    31784	    0	        MATCH
G	                    2023	556.26	    556.26	    0	        23	        23	        0	        MATCH
GR	                    2020	5035350.11	5035350.11	0	        346101	    346101	    0       	MATCH
GR	                    2021	876315.69	876315.69	0	        51817	    51817   	0	        MATCH
GR	                    2022	202940.9	202940.9	0	        10788	    10788	    0	        MATCH
H	                    2018	19499.98	19499.98	0	        363	        363	        0	        MATCH
H	                    2019	2371400.16	2371400.16	0	        67247	    67247	    0	        MATCH
H	                    2020	4483142.67	4483142.67	0	        118342	    118342	    0	        MATCH
H	                    2021	2275267.19	2275267.19	0	        57273	    57273	    0	        MATCH
H	                    2022	2920322.12	2920322.12	0	        71500	    71500	    0	        MATCH
HC	                    2020	13307421.86	13307421.86	0	        1590736	    1590736	    0	        MATCH
HC	                    2021	714181.3	714181.3	0	        137485	    137485	    0	        MATCH
HC	                    2022	192681.2	192681.2	0	        22350	    22350	    0	        MATCH
HK	                    2018	29206.73	29206.73	0	        1142	    1142	    0	        MATCH
HK	                    2019	2621489.07	2621489.07	0	        154046	    154046	    0	        MATCH
HK	                    2020	8354659.31	8354659.31	0	        489225	    489225	    0	        MATCH
HK	                    2021	2116623.63	2116623.63	0	        116353	    116353	    0	        MATCH
HK	                    2022	2379991.96	2379991.96	0	        101776	    101776	    0	        MATCH
HK	                    2023	39.89	    39.89	    0	        1	        1	        0	        MATCH
I	                    2018	18523	    18523	    0	        1930	    1930	    0	        MATCH
I	                    2019	830373.55	830373.55	0	        357930	    357930	    0   	    MATCH
I	                    2020	967215.28	967215.28	0	        329808	    329808	    0   	    MATCH
I	                    2021	680355.13	680355.13	0	        1189693	    1189693	    0	        MATCH
I	                    2022	908130.32	908130.32	0	        350747	    350747	    0	        MATCH
J	                    2018	1149.04	    1149.04	    0	        29	        29	        0	        MATCH
J	                    2019	279478.62	279478.62	0	        5234	    5234	    0	        MATCH
J	                    2020	239489.6	239489.6	0	        3515	    3515	    0	        MATCH
J	                    2021	112508.12	112508.12	0	        1428	    1428	    0	        MATCH
J	                    2022	96178.71	96178.71	0	        816	        816	        0	        MATCH
J	                    2023	98.39	    98.39	    0	        1	        1	        0	        MATCH
K	                    2019	97871.21	97871.21	0	        430	        430	        0	        MATCH
K	                    2020	83008.98	83008.98	0	        552	        552	        0	        MATCH
K	                    2021	44530.84	44530.84	0	        265	        265	        0	        MATCH
K	                    2022	100395.09	100395.09	0	        492	        492	        0	        MATCH
KF	                    2022	2726.07	    2726.07	    0	        51	        51	        0	        MATCH
L	                    2018	30208.82	30208.82	0	        250	        250	        0	        MATCH
L	                    2019	1409282.54	1409282.54	0	        20182	    20182	    0	        MATCH
L	                    2020	2213141.48	2213141.48	0	        29379	    29379	    0	        MATCH
L	                    2021	1474379.19	1474379.19	0	        17690	    17690	    0	        MATCH
L	                    2022	2125445.73	2125445.73	0	        22597	    22597	    0	        MATCH
L	                    2023	178	        178	        0	        3	        3	        0	        MATCH
M	                    2018	10648.92	10648.92	0	        330	        330	        0	        MATCH
M	                    2019	870428.83	870428.83	0	        26049	    26049	    0	        MATCH
M	                    2020	1339625.26	1339625.26	0	        39707	    39707	    0	        MATCH
M	                    2021	556773.36	556773.36	0	        17962	    17962	    0	        MATCH
M	                    2022	798933.88	798933.88	0	        21489	    21489	    0	        MATCH
O	                    2020	-4182.46	-4182.46	0	        -8	        -8	        0	        MATCH
O	                    2022	0	        0	        0	        -1	        -1	        0	        MATCH
P	                    2018	395	        395	        0	        100	        100	        0	        MATCH
P	                    2019	201285.52	201285.52	0	        26273	    26273	    0	        MATCH
P	                    2020	318299.91	318299.91	0	        38066	    38066	    0	        MATCH
P	                    2021	207409.81	207409.81	0	        26665	    26665	    0	        MATCH
P	                    2022	301886.06	301886.06	0	        33195	    33195	    0	        MATCH
P	                    2023	-350.5	    -350.5	    0	        0	        0	        0	        MATCH
R	                    2018	23041.02	23041.02	0	        81	        81	        0	        MATCH
R	                    2019	3078670.34	3078670.34	0	        12823	    12823	    0	        MATCH
R	                    2020	3893471.68	3893471.68	0	        16965	    16965	    0	        MATCH
R	                    2021	3281410.87	3281410.87	0	        12682	    12682	    0	        MATCH
R	                    2022	4182145.68	4182145.68	0	        13325	    13325	    0	        MATCH
R	                    2023	-721.74	    -721.74	    0	        0	        0	        0	        MATCH
RB	                    2019	641404.26	641404.26	0	        10449	    10449	    0	        MATCH
RB	                    2020	1272388.03	1272388.03	0	        35304	    35304	    0	        MATCH
RB	                    2021	679883.89	679883.89	0	        9063	    9063	    0	        MATCH
RB	                    2022	243648.66	243648.66	0	        2474	    2474	    0	        MATCH
RB	                    2023	0	        0	        0	        4	        4	        0	        MATCH
RF	                    2020	7222193.78	7222193.78	0	        406165	    406165	    0	        MATCH
RF	                    2021	4965170.02	4965170.02	0	        309321	    309321	    0	        MATCH
RF	                    2022	3168564.15	3168564.15	0	        80530	    80530	    0	        MATCH
RF	                    2023	545	        545	        0	        0	        0	        0	        MATCH
RM	                    2020	30763.21	30763.21	0	        193200	    193200	    0	        MATCH
RM	                    2021	21877	    21877	    0	        32206	    32206	    0	        MATCH
RM	                    2022	30245.85	30245.85	0	        716515	    716515	    0	        MATCH
S	                    2018	4670.17	    4670.17	    0	        315	        315	        0	        MATCH
S	                    2019	958232.78	958232.78	0	        64727	    64727	    0	        MATCH
S	                    2020	1367903.18	1367903.18	0	        90200	    90200	    0	        MATCH
S	                    2021	991853.64	991853.64	0	        65099	    65099	    0	        MATCH
S	                    2022	907162.19	907162.19	0	        51289	    51289	    0	        MATCH
S	                    2024	405	        405	        0	        6	        6	        0	        MATCH
S	                    2025	1854.5	    1854.5	    0	        10	        10	        0	        MATCH
S	                    2026	72.51	    72.51	    0	        3	        3	        0	        MATCH
SC	                    2019	209657.45	209657.45	0	        319727	    319727	    0	        MATCH
SC	                    2020	264093.4	264093.4	0	        211437	    211437	    0	        MATCH
SC	                    2021	202045.45	202045.45	0	        548577	    548577	    0	        MATCH
SC	                    2022	161407.76	161407.76	0	        221276	    221276	    0	        MATCH
SX	                    2018	6426	    6426	    0	        108	        108	        0	        MATCH
SX	                    2019	2033207.96	2033207.96	0	        22239	    22239	    0   	    MATCH
SX	                    2020	1669637.14	1669637.14	0	        20737	    20737	    0	        MATCH
SX	                    2021	1231536.84	1231536.84	0	        13186	    13186	    0	        MATCH
SX	                    2022	1166893.7	1166893.7	0	        10431	    10431	    0	        MATCH
SX	                    2023	185.48	    185.48	    0	        2	        2	        0	        MATCH
T	                    2018	13905.9	    13905.9	    0	        115	        115	        0	        MATCH
T	                    2019	593588.14	593588.14	0	        7718	    7718	    0	        MATCH
T	                    2020	2053368.31	2053368.31	0	        25270	    25270	    0	        MATCH
T	                    2021	1172633.72	1172633.72	0	        14787	    14787	    0	        MATCH
T	                    2022	1262192.78	1262192.78	0	        13198	    13198	    0	        MATCH
T	                    2023	-128	    -128	    0	        0	        0	        0	        MATCH
T	                    2026	0	        0	        0	        0	        0	        0	        MATCH
TC	                    2019	775	        775	        0	        -1750	    -1750	    0	        MATCH
TC	                    2021	868	        868	        0	        1440	    1440	    0	        MATCH
TG	                    2018	76234.36	76234.36	0	        624	        624	        0	        MATCH
TG	                    2019	3008650.01	3008650.01	0	        50144	    50144	    0	        MATCH
TG	                    2020	6123101.7	6123101.7	0	        108851	    108851	    0	        MATCH
TG	                    2021	3409377.88	3409377.88	0	        55962	    55962	    0	        MATCH
TG	                    2022	3162707.44	3162707.44	0	        43940	    43940	    0	        MATCH
TG	                    2023	0	        0	        0	        2	        2	        0	        MATCH
U	                    2019	355116.17	355116.17	0	        7752	    7752	    0	        MATCH
U	                    2020	1168653	    1168653	    0	        26367	    26367	    0	        MATCH
U	                    2021	1240253.57	1240253.57	0	        28664	    28664	    0	        MATCH
U	                    2022	1029429.69	1029429.69	0	        19991	    19991	    0	        MATCH
US	                    2021	1367860.22	1367860.22	0	        53255	    53255	    0	        MATCH
US	                    2022	3817944.26	3817944.26	0	        132776	    132776	    0	        MATCH
V	                    2019	1139.2	    1139.2	    0	        17	        17	        0	        MATCH
VC	                    2019	18	        18	        0	        0	        0	        0	        MATCH
VC	                    2021	400	        400	        0	        0	        0	        0	        MATCH
VT	                    2018	314.93	    314.93	    0	        5	        5	        0	        MATCH
VT	                    2019	985832.44	985832.44	0	        49877	    49877	    0	        MATCH
VT	                    2020	1585066.52	1585066.52	0	        60189	    60189	    0	        MATCH
VT	                    2021	753025.31	753025.31	0	        36924	    36924	    0	        MATCH
VT	                    2022	672821	    672821	    0	        32453	    32453	    0	        MATCH
VT	                    2023	2765.69	    2765.69	    0	        41	        41	        0	        MATCH
W	                    2019	124323.02	124323.02	0	        7274	    7274	    0	        MATCH
W	                    2020	374773.28	374773.28	0	        14950	    14950	    0	        MATCH
W	                    2021	186911.91	186911.91	0	        6296	    6296	    0	        MATCH
W	                    2022	216934.16	216934.16	0	        6969	    6969	    0	        MATCH
W	                    2023	0	        0	        0	        0	        0	        0	        MATCH
X	                    2018	0	        0	        0	        0	        0	        0	        MATCH
X	                    2019	1371.53	    1371.53	    0	        534	        534	        0	        MATCH
X	                    2020	43328.13	43328.13	0	        7348	    7348	    0	        MATCH
X	                    2021	20000.98	20000.98	0	        1370	    1370	    0	        MATCH
X	                    2022	16416.71	16416.71	0	        2379	    2379	    0	        MATCH
X	                    2023	90.84	    90.84	    0	        7	        7	        0	        MATCH

 */