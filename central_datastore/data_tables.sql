create table products(
id INTEGER,
COMPANY_CODE VARCHAR,
CATEGORY_DESC VARCHAR,
SUBCATEGORY_DESC VARCHAR,
BRAND_DESC VARCHAR,
BASEPACK VARCHAR,
BASEPACK_DESC VARCHAR,
BARCODE VARCHAR,
PRIMARY KEY (id)
);

create table data_stream(
id INTEGER,
POS_Application_Name varchar,
STOREID varchar,
MACID varchar,
BILLNO varchar,
BARCODE varchar,
GUID varchar,
CREATED_STAMP timestamp without time zone,
CAPTURED_WINDOW varchar,
UPDATE_STAMP timestamp without time zone,
PRIMARY KEY (id)
);
