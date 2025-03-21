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


✅ Step 3: Plot Pareto Chart (Bar + Line)
python
Copy
Edit
import matplotlib.pyplot as plt

fig, ax1 = plt.subplots(figsize=(12, 6))

# Bar plot for fallout counts
ax1.bar(fallout_df['Fallout_Reason'], fallout_df['Fallout_Count'], color='salmon')
ax1.set_xlabel('Fallout Reason')
ax1.set_ylabel('Fallout Count', color='red')
ax1.tick_params('y', colors='red')
ax1.set_xticklabels(fallout_df['Fallout_Reason'], rotation=45, ha='right')

# Line plot for cumulative percentage
ax2 = ax1.twinx()
ax2.plot(fallout_df['Fallout_Reason'], fallout_df['Cumulative_Percent'], color='blue', marker='o')
ax2.set_ylabel('Cumulative %', color='blue')
ax2.tick_params('y', colors='blue')
ax2.axhline(80, color='grey', linestyle='--')  # 80% Reference Line

plt.title('Pareto Chart - Order Fallout Root Cause Analysis')
plt.tight_layout()
plt.show()


✅ Output: Visual Insights
Red Bars → Fallout Counts per reason
Blue Line → Cumulative impact (%)
Dashed 80% Line → Highlights the major contributors causing 80% of fallout
✅ Optional - Display inside Databricks notebook
python
Copy
Edit
display(fallout_df)  # Databricks native table
Or, embed the above Matplotlib chart directly in Databricks.
