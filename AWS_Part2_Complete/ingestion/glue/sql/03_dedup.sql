SELECT *, ROW_NUMBER() OVER(PARTITION BY document_date,buyer_name,document_type,reference_no,seller_name,product_category,quantity,item_description,invoice_year,unit_price ORDER BY amount DESC) duplicate_rank
FROM validated WHERE failure_reason=''
