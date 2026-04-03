import pandas as pd
from sqlalchemy import create_engine, text
import shutil
import time
import os
import logging

# ----------------------------------------------------------------------------------------------------------------
# PATH CONFIGURATION
# ----------------------------------------------------------------------------------------------------------------
EXCEL_PATH = r"D:\Projects\Retail_Sales_Automation_Pipeline\Data\excel\superstore_sales_analysis.xlsx"

# Temporary file (copy)
TEMP_EXCEL_PATH = r"D:\Projects\Retail_Sales_Automation_Pipeline\Data\excel\superstore_sales_analysis_temp.xlsx"

DATABASE_URL = "mysql+pymysql://root:1234@localhost/retail_analytics_db"

TABLE_NAME = "fact_sales"
UNIQUE_KEY = "Row_ID"

# ----------------------------------------------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ----------------------------------------------------------------------------------------------------------------
LOG_PATH = r"D:\Projects\Retail_Sales_Automation_Pipeline\Logs\sales_update.log"

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Function to update sales data from Excel to MySQL database
def update_sales_from_excel(excel_path=EXCEL_PATH):

    logger.info("Sales Update Script Started")

# ---------------------------------------------------------------------------------------
# COPY EXCEL TO TEMP FILE
# ---------------------------------------------------------------------------------------
    for i in range(5):
        try:
            shutil.copy2(excel_path, TEMP_EXCEL_PATH)
            logger.info(f"Excel copied successfully to temp file: {TEMP_EXCEL_PATH}")
            break
        except PermissionError:
            logger.warning("Excel file is open/locked, retrying...")
            time.sleep(2)
    else:
        logger.error("Failed to copy Excel file after 5 retries.")
        return

    try:
# ------------------------------------------------------------------------------------------
# READ EXCEL DATA
# ------------------------------------------------------------------------------------------
        df = pd.read_excel(TEMP_EXCEL_PATH)

        df.columns = df.columns.str.strip() #Clean column names(remove spaces)

        if UNIQUE_KEY not in df.columns:   # if not found, stop script
            logger.error(f"{UNIQUE_KEY} column not found in Excel.")
            return

        df[UNIQUE_KEY] = df[UNIQUE_KEY].dropna().astype(int) # Ensure unique key is integer 

        logger.info(f"Excel read successfully. Total rows: {len(df)}")

# ------------------------------------------------------------------------------------------------
# DATABASE CONNECTION 
# ------------------------------------------------------------------------------------------------
        engine = create_engine(DATABASE_URL) # create connection to mysql database

        with engine.begin() as conn: # Database connection with auto commit transaction

# ------------------------------------------------------------------------------------------------
# FETCH EXISTING IDS FROM DATABASE AND EXCEL FOR COMPARISON 
# ------------------------------------------------------------------------------------------------
            existing_ids = conn.execute(
                text(f"SELECT {UNIQUE_KEY} FROM {TABLE_NAME}")
            ).fetchall()

            existing_ids = set([row[0] for row in existing_ids])

            excel_ids = set(df[UNIQUE_KEY].dropna().astype(int))

            logger.info(f"Fetched {len(existing_ids)} Row_IDs from DB")
            logger.info(f"Fetched {len(excel_ids)} Row_IDs from Excel")

# -------------------------------------------------------------------------------------------------
# FIND NEW / UPDATE / DELETE (ROWS BY COMPARING EXCEL AND DATABASE IDS)
# -------------------------------------------------------------------------------------------------
            new_rows = df[~df[UNIQUE_KEY].isin(existing_ids)]
            update_rows = df[df[UNIQUE_KEY].isin(existing_ids)]
            deleted_ids = existing_ids - excel_ids

            logger.info(f"New rows found: {len(new_rows)}")
            logger.info(f"Rows to update: {len(update_rows)}")
            logger.info(f"Rows to delete: {len(deleted_ids)}")

# ------------------------------------------------------------------------------------------------
# INSERT NEW ROWS FROM EXCEL TO DATABASE
# ------------------------------------------------------------------------------------------------
            if not new_rows.empty:
                new_rows.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
                logger.info(f"Inserted {len(new_rows)} new rows into {TABLE_NAME}")
            else:
                logger.info("No new rows to insert.")

# ------------------------------------------------------------------------------------------------
# UPDATE EXISTING ROWS IN DATABASE BASED ON EXCEL DATA 
# ------------------------------------------------------------------------------------------------
            updated_count = 0

            for _, row in update_rows.iterrows():
                conn.execute(
                    text(f"""
                        UPDATE {TABLE_NAME}
                        SET Sales = :sales,
                            Profit = :profit,
                            Quantity = :quantity,
                            Discount = :discount
                        WHERE {UNIQUE_KEY} = :row_id
                    """),
                    {
                        "sales": float(row["Sales"]),
                        "profit": float(row["Profit"]),
                        "quantity": int(row["Quantity"]),
                        "discount": float(row["Discount"]),
                        "row_id": int(row[UNIQUE_KEY])
                    }
                )
                updated_count += 1

            logger.info(f"Updated {updated_count} rows in {TABLE_NAME}")

# ---------------------------------------------------------------------------------------
# DELETE REMOVED ROWS FROM DATABASE 
# ---------------------------------------------------------------------------------------
            deleted_count = 0

            for rid in deleted_ids:
                conn.execute(
                    text(f"DELETE FROM {TABLE_NAME} WHERE {UNIQUE_KEY} = :rid"),
                    {"rid": int(rid)}
                )
                deleted_count += 1

            logger.info(f"Deleted {deleted_count} rows from {TABLE_NAME}")

    except Exception as e:
        logger.error(f"Error occurred during sales update process: {str(e)}")

    finally:
# -------------------------------------------------------------------------------------------------
# DELETE TEMP FILE AND FINISH LOGGING 
# -------------------------------------------------------------------------------------------------
        if os.path.exists(TEMP_EXCEL_PATH):
            os.remove(TEMP_EXCEL_PATH)
            logger.info("Temporary Excel file deleted successfully.")

        logger.info("Sales Update Script Finished\n" + "-" * 60)

# Run the update function when the script is executed directly from task scheduler or command line
if __name__ == "__main__":
    update_sales_from_excel()

