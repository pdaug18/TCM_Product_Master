select * FROM "GOLD_DATA"."DIM"."DIM_CALENDAR"

WITH working_days AS (
    /* This CTE generates a list of working days from the DIM_CALENDAR table, assigning a sequential number to each working day. */
    SELECT
        "CALENDAR_DATE",
        ROW_NUMBER() OVER (ORDER BY "CALENDAR_DATE") AS working_day_num
    FROM "GOLD_DATA"."DIM"."DIM_CALENDAR"
    WHERE "IS_WEEKDAY" = TRUE AND "IS_HOLIDAY" = FALSE
)
SELECT
    src.*,
    CASE
        WHEN (src."Inventory_Stock_Status" = 'S' OR src."Planned_Classification" IN ('AS','1A','KT'))
             AND wd5."CALENDAR_DATE" >= src."Date_Order_Promised"
        THEN src."Date_Order_Promised"
        WHEN src."Date_Order_Requested" = src."Date_Order_Promised"
        THEN src."Date_Order_Requested"
        ELSE wd10."CALENDAR_DATE"
    END AS "Date_Order_Acknowledged"
FROM $T{CALC_FINAL} src
LEFT JOIN working_days wd_do
    ON wd_do."CALENDAR_DATE" = (
        SELECT MIN(w."CALENDAR_DATE")
        FROM working_days w
        WHERE w."CALENDAR_DATE" >= src."Date_Ordered"
    )
LEFT JOIN working_days wd5
    ON wd5.working_day_num = wd_do.working_day_num + 5
LEFT JOIN working_days wd_dp
    ON wd_dp."CALENDAR_DATE" = (
        SELECT MIN(w."CALENDAR_DATE")
        FROM working_days w
        WHERE w."CALENDAR_DATE" >= src."Date_Order_Promised"
    )
LEFT JOIN working_days wd10
    ON wd10.working_day_num = wd_dp.working_day_num + 10