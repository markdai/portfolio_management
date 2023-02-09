CREATE VIEW positions (
  SYMBOL,
  DESCRIPTION,
  INVESTMENT_TYPE,
  COST_DOLLARS,
  DOLLARS,
  UNITS,
  LAST_UPDATED,
  MKT_VALUE,
  GAIN_PER_SHARE,
  GAIN_TOTAL,
  GAIN_PERCENTAGE
)
AS
SELECT
  t1.SYMBOL AS SYMBOL,
  t2.FULL_NAME AS DESCRIPTION,
  t1.INVESTMENT_TYPE AS INVESTMENT_TYPE,
  t1.COST_DOLLARS AS COST_DOLLARS,
  ROUND(t2.PREV_CLOSE,2) AS DOLLARS,
  t1.UNITS AS UNITS,
  t2.LAST_UPDATED AS LAST_UPDATED,
  ROUND(t2.PREV_CLOSE*t1.UNITS,2) AS MKT_VALUE,
  ROUND(t2.PREV_CLOSE-t1.COST_DOLLARS,2) AS GAIN_PER_SHARE,
  ROUND((t2.PREV_CLOSE-t1.COST_DOLLARS)*t1.UNITS,2) AS GAIN_TOTAL,
  ROUND((t2.PREV_CLOSE-t1.COST_DOLLARS)/t1.COST_DOLLARS,2) AS GAIN_PERCENTAGE
FROM tmp_holdings t1
LEFT JOIN watch_list t2
  ON t1.SYMBOL = t2.SYMBOL;