# Step 1: Import Snowpark packages
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, sum, lit, year, quarter, to_date, concat, count

# Step 2: Set up current database schema and perform aggregation

def main(session:snowpark.Session):

    session.sql('USE SCHEMA SNOWPARK_DB.TRANSFORMED').collect()

    ## Aggregation 1: Create a global sales table with delivered status
    df_global_sales_order_delivered = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")\
                                                .filter(col("SHIPPING_STATUS") == 'Delivered')

    df_global_sales_order_delivered.write.mode("overwrite").save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_DELIVERED")

    ## Aggregation 2: Create a global sales table by aggregating the sales by brand
    df_global_sales_order_brand = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")\
                                            .groupBy(col("MOBILE_BRAND"), col("MOBILE_MODEL"))\
                                            .agg(
                                                sum(col("TOTAL_PRICE_USD")).alias("TOTAL_SALES_AMOUNT"),
                                                count(col("ORDER_ID")).alias("TOTAL_ORDERS"),
                                                sum(col("QUANTITY")).alias("TOTAL_QUANTITY")
                                            )

    df_global_sales_order_brand.write.mode("overwrite").save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_BRAND")

    ## Aggregation 3: Creating the global sales table by aggregating the sales by country
    df_global_sales_order_country = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")\
                                            .groupBy(col("COUNTRY"))\
                                            .agg(sum(col("TOTAL_PRICE_USD")).alias("TOTAL_SALES_AMOUNT"))

    df_global_sales_order_country.write.mode("overwrite").save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_COUNTRY")

    ## Aggregation 4: Creating the global sales table by quarter
    df_global_sales_order_quarter = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")\
                                            .groupBy(year(to_date(col("ORDER_DATE"))).alias("YEAR"),
                                                    concat(lit("Q"), quarter(to_date(col("ORDER_DATE")))).alias("QUARTER"))\
                                                    .agg(sum(col("QUANTITY")).alias("TOTAL_SALES_VOLUME"),
                                                            sum(col("TOTAL_PRICE_USD")).alias("TOTAL_SALES_AMOUNT"))

    df_global_sales_order_quarter.write.mode("overwrite").save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_QUARTER")


    return "Data Aggregated and Loaded into Curated Schema successfully."








