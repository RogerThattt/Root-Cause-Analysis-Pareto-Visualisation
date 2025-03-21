# Root-Cause-Analysis-Pareto-Visualisation

✅ Step 1: Aggregate Fallout Counts by Reason (PySpark / SQL)
sql
Copy
Edit
SELECT 
  Fallout_Reason, 
  COUNT(DISTINCT Order_ID) AS Fallout_Count
FROM 
  order_fallout_table
WHERE 
  Fallout_Flag = 'Y'
GROUP BY 
  Fallout_Reason
ORDER BY 
  Fallout_Count DESC
Convert this into a Pandas DataFrame:

python
Copy
Edit
fallout_df = spark.sql("""SELECT Fallout_Reason, COUNT(DISTINCT Order_ID) AS Fallout_Count
                          FROM order_fallout_table
                          WHERE Fallout_Flag = 'Y'
                          GROUP BY Fallout_Reason
                          ORDER BY Fallout_Count DESC""").toPandas()


✅ Step 2: Calculate Cumulative Percentage
python
Copy
Edit
fallout_df['Cumulative_Sum'] = fallout_df['Fallout_Count'].cumsum()
fallout_df['Cumulative_Percent'] = 100 * fallout_df['Cumulative_Sum'] / fallout_df['Fallout_Coun
