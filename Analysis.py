import pandas as pd
import sqlite3


def clean_data():
    #Creating Database connection
    conn = sqlite3.connect('inventory.db')
    # Checking the names of tables in our database
    tables = pd.read_sql_query("SELECT name FROM sqlite_master where type = 'table'",conn)

    vendor_purchase_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT VendorNumber,
            VendorName,
            SUM(Freight) AS Freight_Cost
        FROM Vendor_invoice
        GROUP BY VendorNumber, VendorName
    ),

    PurchaseSummary AS (
        SELECT Brand,
            Description AS ProductDescription,
            VendorName,
            VendorNumber,
            PurchasePrice AS TotalPurchasePrice,
            SUM(Quantity) AS TotalPurchaseQuantity,
            SUM(Dollars) AS TotalPurchaseAmount
        FROM purchases
        GROUP BY Brand, Description, VendorName, VendorNumber, PurchasePrice
    ),

    PurchasePricesSummary AS (
        SELECT Brand,
            Price AS ActualPrice,
            Volume AS Volume_ml
        FROM purchase_prices
    )

    SELECT
        p.Brand,
        p.ProductDescription,
        p.VendorName,
        p.VendorNumber,
        p.TotalPurchasePrice,
        pp.ActualPrice,
        p.TotalPurchaseQuantity,
        pp.Volume_ml,
        vi.Freight_Cost,
        p.TotalPurchaseAmount
    FROM PurchaseSummary AS p
    JOIN PurchasePricesSummary AS pp
        ON p.Brand = pp.Brand
    JOIN FreightSummary AS vi
        ON vi.VendorNumber = p.VendorNumber
    ORDER BY TotalPurchasePrice
    """, conn)

    return vendor_purchase_summary

if __name__ == '__main__':
    clean_data()


