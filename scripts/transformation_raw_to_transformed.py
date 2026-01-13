# Step 1: Import Snowpark Packages

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, split, current_timestamp, concat, count

# Step 2: Set up current database and schema, and perform transformation

def main(session: snowpark.Session):
    
    session.sql("USE SCHEMA SNOWPARK_DB.RAW").collect()

    # Step 3: Transformations
    
    ##Transformation 1: Ordering the columns, renaming the coluns and adding country column
    
    ### India Sales Order
    df_india_sales_order = session.sql("""
                                        SELECT
                                            ORDER_ID,
                                            CUSTOMER_NAME,
                                            MOBILE_MODEL,
                                            QUANTITY,
                                            CAST(PRICE_PER_UNIT AS NUMERIC)/90.31 AS PRICE_PER_UNIT_USD,
                                            CAST(TOTAL_PRICE AS NUMERIC)/90.31 AS TOTAL_PRICE_USD,
                                            PROMOTION_CODE,
                                            CAST(ORDER_AMOUNT AS NUMERIC)/90.31 AS ORDER_AMOUNT_USD,
                                            CAST(GST AS NUMERIC)/90.31 AS GST_USD,
                                            ORDER_DATE,
                                            PAYMENT_STATUS,
                                            SHIPPING_STATUS,
                                            PAYMENT_METHOD,
                                            PAYMENT_PROVIDER,
                                            MOBILE,
                                            DELIVERY_ADDRESS
                                        FROM SNOWPARK_DB.RAW.INDIA_SALES_ORDER""")
    #### Renaming GST and MOBILE columns
    df_india_sales_order_renamed = df_india_sales_order.rename("GST_USD","TAX").rename("MOBILE","CONTACT_NUMBER")
                                    

    #### Adding a country column
    df_india_sales_order_country = df_india_sales_order_renamed.withColumn("COUNTRY", lit("INDIA"))

    ### USA Sales Order
    df_usa_sales_order = session.sql("""
                                        SELECT
                                            ORDER_ID,
                                            CUSTOMER_NAME,
                                            MOBILE_MODEL,
                                            QUANTITY,
                                            PRICE_PER_UNIT,
                                            TOTAL_PRICE,
                                            PROMOTION_CODE,
                                            ORDER_AMOUNT,
                                            TAX,
                                            ORDER_DATE,
                                            PAYMENT_STATUS,
                                            SHIPPING_STATUS,
                                            PAYMENT_METHOD,
                                            PAYMENT_PROVIDER,
                                            PHONE,
                                            DELIVERY_ADDRESS
                                        FROM SNOWPARK_DB.RAW.USA_SALES_ORDER""")
    #### Renaming PHONE column
    df_usa_sales_order_renamed = df_usa_sales_order.rename("PHONE", "CONTACT_NUMBER")\
                                                    .rename("PRICE_PER_UNIT","PRICE_PER_UNIT_USD")\
                                                    .rename("TOTAL_PRICE","TOTAL_PRICE_USD")\
                                                    .rename("ORDER_AMOUNT","ORDER_AMOUNT_USD")

    #### Adding COUNTRY column 
    df_usa_sales_order_country = df_india_sales_order_renamed.withColumn("COUNTRY",lit("USA"))

    ### FRANCE Sales Order
    df_france_sales_order = session.sql("""
                                        SELECT
                                            ORDER_ID,
                                            CUSTOMER_NAME,
                                            MOBILE_MODEL,
                                            QUANTITY,
                                            CAST(PRICE_PER_UNIT AS NUMERIC)/0.86 AS PRICE_PER_UNIT_USD,
                                            CAST(TOTAL_PRICE AS NUMERIC)/0.86 AS TOTAL_PRICE_USD,
                                            PROMOTION_CODE,
                                            CAST(ORDER_AMOUNT AS NUMERIC)/0.86 AS ORDER_AMOUNT_USD,
                                            CAST(TAX AS NUMERIC)/90.31 AS TAX,
                                            ORDER_DATE,
                                            PAYMENT_STATUS,
                                            SHIPPING_STATUS,
                                            PAYMENT_METHOD,
                                            PAYMENT_PROVIDER,
                                            PHONE,
                                            DELIVERY_ADDRESS
                                        FROM SNOWPARK_DB.RAW.FRANCE_SALES_ORDER""")

    #### Renaming PHONE columns
    df_france_sales_order_renamed = df_france_sales_order.rename("PHONE","CONTACT_NUMBER")

    #### Adding a COUNTRY field
    df_france_sales_order_country = df_france_sales_order_renamed.withColumn("COUNTRY", lit("FRANCE"))

    ## Transformation 2: Union of Dataframes
    df_india_usa_sales_order = df_india_sales_order_country.union(df_usa_sales_order_country)
    df_union_sales_order = df_india_usa_sales_order.union(df_france_sales_order_country)

    ## Transformation 3: Fill in the mssing values
    df_union_sales_order_fill = df_union_sales_order.fillna("NA", subset ="promotion_code")

    ## Transformation 4: Split the MOBILE_MODEL columns
    df_union_sales_order_split = df_union_sales_order_fill\
                                .withColumn("MOBILE_BRAND", split(col("MOBILE_MODEL"),lit("/")).getItem(0).cast("string"))\
                                .withColumn("MOBILE_VERSION", split(col("MOBILE_MODEL"), lit("/")).getItem(1).cast("string"))\
                                .withColumn("MOBILE_COLOR", split(col("MOBILE_MODEL"), lit("/")).getItem(2).cast("string"))\
                                .withColumn("MOBILE_RAM", split(col("MOBILE_MODEL"), lit("/")).getItem(3).cast("string"))\
                                .withColumn("MOBILE_MEMORY", split(col("MOBILE_MODEL"), lit("/")).getItem(4).cast("string"))

    ## Transformation 5: Rearranging the columns and inserting current dts column
    df_union_sales_order_final = df_union_sales_order_split.select(col("ORDER_ID"),
                                                                   col("CUSTOMER_NAME"),
                                                                   col("MOBILE_BRAND"),
                                                                   col("MOBILE_VERSION").alias("MOBILE_MODEL"),
                                                                   col("MOBILE_COLOR"),
                                                                   col("MOBILE_RAM"),
                                                                   col("MOBILE_MEMORY"),
                                                                   col("QUANTITY"),
                                                                   col("PRICE_PER_UNIT_USD"),
                                                                   col("PROMOTION_CODE"),
                                                                   col("TOTAL_PRICE_USD"),
                                                                   col("ORDER_AMOUNT_USD"),
                                                                   col("TAX"),
                                                                   col("ORDER_DATE"),
                                                                   col("PAYMENT_STATUS"),
                                                                   col("SHIPPING_STATUS"),
                                                                   col("PAYMENT_METHOD"),
                                                                   col("PAYMENT_PROVIDER"),
                                                                   col("CONTACT_NUMBER"),
                                                                   col("COUNTRY"),
                                                                   col("DELIVERY_ADDRESS")
                                                                  ).withColumn("INSERT_DTS", current_timestamp())\
                                                                    .withColumn("UNIQUE_ORDER_ID",concat(col("COUNTRY"),col("ORDER_ID")))
    
    df_union_sales_order_final = df_union_sales_order_final.drop_duplicates("UNIQUE_ORDER_ID")
    # Step 4: Load the final dataframe into transformed schema

    df_union_sales_order_final.write.mode("overwrite").save_as_table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")

    return "Data transformed and loaded into transformed layer successfully."




















    
