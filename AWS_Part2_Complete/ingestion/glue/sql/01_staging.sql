SELECT
 trim(CAST(document_date AS STRING)) AS document_date,
 upper(trim(regexp_replace(CAST(buyer_name AS STRING), '\\s+', ' '))) AS buyer_name,
 upper(trim(CAST(document_type AS STRING))) AS document_type,
 trim(CAST(reference_no AS STRING)) AS reference_no,
 upper(trim(regexp_replace(CAST(seller_name AS STRING), '\\s+', ' '))) AS seller_name,
 upper(trim(CAST(product_category AS STRING))) AS product_category,
 CAST(quantity AS DOUBLE) AS quantity,
 trim(CAST(item_description AS STRING)) AS item_description,
 CAST(invoice_year AS INT) AS invoice_year,
 CAST(unit_price AS DOUBLE) AS unit_price,
 CAST(amount AS DOUBLE) AS amount,
 to_date(document_date, 'yyyy-MM-dd') AS document_date_parsed,
 source_file
FROM raw_data
