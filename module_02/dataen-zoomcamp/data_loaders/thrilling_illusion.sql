-- Docs: https://docs.mage.ai/guides/sql-blocks
select to_char(lpep_pickup_date, 'YYYY-MM-DD') from mage.green_taxi group by lpep_pickup_date;