import pandas as pd
import sqlite3
from db_ingestion import ingest_db
import logging


logging.basicConfig(
    filename='logs/vendor_analysis_db.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force = True
)

def create_vendor_summary(conn):
    '''This Function will select the subset of the large dataset, only the variables needed for Analysis '''
    vendor_purchase_summary = pd.read_sql_query(
        """
        WITH FreightSummary AS (
            SELECT
                VendorNumber,
                VendorName,
                SUM(Freight) AS Freight_Cost
            FROM Vendor_invoice
            GROUP BY VendorNumber, VendorName
        ),

        PurchaseSummary AS (
            SELECT
                p.Brand,
                p.Description AS ProductDescription,
                p.VendorName,
                p.VendorNumber,
                p.PurchasePrice AS TotalPurchasePrice,
                pp.Price AS ActualPrice,
                pp.Volume AS Volume_ml,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseAmount
            FROM purchases AS p
            JOIN purchase_prices AS pp
                ON p.Brand = pp.Brand 
            GROUP BY
                p.Brand,
                p.Description,
                p.VendorName,
                p.VendorNumber,
                p.PurchasePrice,
                pp.Price,
                pp.Volume
        ),

        SalesSummary AS (
            SELECT
                Brand,
                VendorNo,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(ExciseTax) AS TotalExciseDuty
            FROM Sales
            GROUP BY Brand, VendorNo
        )

        SELECT
            ps.Brand,
            ps.ProductDescription,
            ps.VendorName,
            ps.VendorNumber,
            ps.TotalPurchasePrice,
            ps.ActualPrice,
            ps.TotalPurchaseQuantity,
            ps.Volume_ml,
            vi.Freight_Cost,
            ps.TotalPurchaseAmount,
            s.TotalSalesPrice,
            s.TotalSalesDollars,
            s.TotalSalesQuantity,
            s.TotalExciseDuty
        FROM PurchaseSummary AS ps
        JOIN SalesSummary AS s
            ON s.VendorNo = ps.VendorNumber
            AND s.Brand = ps.Brand
        JOIN FreightSummary AS vi
            ON vi.VendorNumber = ps.VendorNumber
        ORDER BY ps.TotalPurchaseAmount DESC
        """,
        conn
    )

    

    return vendor_purchase_summary

def clean_data(df):
      
      df["VendorName"] = df["VendorName"].str.strip() # Removing Whitespace
      df["Volume_ml"] = df["Volume_ml"].astype("Float64") # Converting Volume_mlfrom string to float Value
      df.drop_duplicates(inplace= True) # To Drop Duplicate values

      #Adding new columns to the dataframe
      df["GrossProfit"]= df["TotalPurchaseAmount"]- df["TotalSalesDollars"]  # To Know our Profit or Loss
      df["ProfitMargin"] = (df["GrossProfit"]/df["TotalSalesDollars"])*100 # Percent of Profit/Loss
      df["StockTurnover"] = df["TotalSalesQuantity"]/df["TotalPurchaseQuantity"] #Tells us the remaining stock
      df["SalestoPurchaseRatio"] = df["TotalSalesDollars"]/df["TotalPurchaseAmount"] # Comparison of Sales to Purchase Amount
      df = df[df["TotalPurchaseAmount"]>0] # Remove values where Purchase Amount is 0

      return df     
        


if __name__=='__main__':

    # creating database connection
    conn= sqlite3.connect('inventory.db')

    logging.info("Creating Vendor Summary Table")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info("Cleaning Data.....")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info("Ingesting data.....")
    ingest_db(clean_df,'vendor_sales_summary',conn,"replace")
    logging.info('Completed')
    

