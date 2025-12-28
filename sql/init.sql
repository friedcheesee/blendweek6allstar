CREATE TABLE sales (
    "index" TEXT,
    "Order ID" TEXT,
    "Date" TEXT,
    "Status" TEXT,
    "Fulfilment" TEXT,
    "Sales Channel " TEXT,
    "ship-service-level" TEXT,
    "Style" TEXT,
    "SKU" TEXT,
    "Category" TEXT,
    "Size" TEXT,
    "ASIN" TEXT,
    "Courier Status" TEXT,
    "Qty" TEXT,
    "currency" TEXT,
    "Amount" TEXT,
    "ship-city" TEXT,
    "ship-state" TEXT,
    "ship-postal-code" TEXT,
    "ship-country" TEXT,
    "promotion-ids" TEXT,
    "B2B" TEXT,
    "fulfilled-by" TEXT,
    "Unnamed: 22" TEXT
);

COPY sales
FROM '/data/sales.csv'
DELIMITER ','
CSV HEADER;
