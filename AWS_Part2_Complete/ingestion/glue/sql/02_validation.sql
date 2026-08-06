WITH counted AS (SELECT *, COUNT(buyer_name) OVER(PARTITION BY buyer_name) buyer_frequency, COUNT(document_type) OVER(PARTITION BY document_type) type_frequency, COUNT(product_category) OVER(PARTITION BY product_category) category_frequency FROM staging)
SELECT *, concat_ws('|',
 CASE WHEN document_date_parsed IS NULL THEN 'INVALID_DOCUMENT_DATE' END,
 CASE WHEN document_date_parsed IS NOT NULL AND (document_date < '{{START_DATE}}' OR document_date > '{{END_DATE}}') THEN 'DATE_OUT_OF_SCOPE' END,
 CASE WHEN document_type <> '{{TARGET_DOCUMENT_TYPE}}' OR type_frequency < {{MIN_CATEGORY_FREQUENCY}} THEN 'INVALID_DOCUMENT_TYPE' END,
 CASE WHEN buyer_name IS NULL OR buyer_name='' OR buyer_frequency < {{MIN_CATEGORY_FREQUENCY}} THEN 'INVALID_BUYER' END,
 CASE WHEN product_category IS NULL OR product_category='' OR category_frequency < {{MIN_CATEGORY_FREQUENCY}} THEN 'INVALID_PRODUCT_CATEGORY' END,
 CASE WHEN reference_no IS NULL OR reference_no='' THEN 'MISSING_REFERENCE_NO' END,
 CASE WHEN seller_name IS NULL OR seller_name='' THEN 'MISSING_SELLER_NAME' END,
 CASE WHEN item_description IS NULL OR item_description='' THEN 'MISSING_ITEM_DESCRIPTION' END,
 CASE WHEN quantity IS NULL OR quantity<=0 THEN 'INVALID_QUANTITY' END,
 CASE WHEN amount IS NULL OR amount<=0 THEN 'INVALID_AMOUNT' END,
 CASE WHEN invoice_year IS NULL OR invoice_year<>year(document_date_parsed) THEN 'INVOICE_YEAR_MISMATCH' END,
 CASE WHEN unit_price IS NOT NULL AND unit_price<=0 THEN 'INVALID_UNIT_PRICE' END
 ) failure_reason
FROM counted
