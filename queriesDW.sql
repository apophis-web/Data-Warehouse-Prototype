#QUESTION 1
select PRODUCT_NAME as Products, quarter_ as Quarter_, month_ as Month_, sum(sales) as Sales
from facttable FACTTABLE join date_ on (FACTTABLE.FK_date = datee) join product PRODUCTS on (PRODUCTS.PRODUCT_ID = FACTTABLE.FK_PRODUCT_ID)
group by FACTTABLE.FK_PRODUCT_ID, quarter_, month_;

#QUESTION 2
select PRODUCT_NAME, STORE_NAME, sum(sales) from facttable FACTTABLE
join product PRODUCTS on (FACTTABLE.FK_PRODUCT_ID = PRODUCTS.PRODUCT_ID )
join store STORE on (STORE.STORE_ID = FACTTABLE.FK_STORE_ID)
group by FACTTABLE.FK_PRODUCT_ID, STORE_ID;

#QUESTION 3
select PRODUCT_NAME, sum(sales) as TOTAL from facttable FACTTABLE
join date_ on (FACTTABLE.FK_date = datee)
join product PRODUCTS on (PRODUCTS.PRODUCT_ID = FACTTABLE.FK_PRODUCT_ID)
where dayname(datee) in ("Sunday", "Saturday")
group by product_name, dayname(datee)
order by TOTAL desc limit 5;

#QUESTION 4
select FK_PRODUCT_ID as PID, 
sum(case when quarter_ = 1 then quantity else 0 end) as Quarter1, 
sum(case when quarter_ = 2 then quantity else 0 end) as Quarter2, 
sum(case when quarter_ = 3 then quantity else 0 end) as Quarter3,
sum(case when quarter_ = 4 then quantity else 0 end) as Quarter4
from facttable natural join date_ group by FK_PRODUCT_ID;

#QUESTION 5
select FK_PRODUCT_ID as PID, 
sum(case when month_ >= 6 then quantity else 0 end) as First_Half, 
sum(case when month_ <= 6 then quantity else 0 end) as Second_Half,
sum(case when month_ <= 12 then quantity else 0 end) as Total_Yearly_Sale
from facttable natural join date_ group by FK_PRODUCT_ID;

#QUESTION 6
select * from product where (PRODUCT_NAME = "Tomatoes");

#QUESTION 7
DROP TABLE if exists STOREANALYSIS_MV;
create view STOREANALYSIS_MV AS SELECT FK_STORE_ID, FK_PRODUCT_ID, sum(sales) as SALES_TOTAL from facttable inner join store on store.STORE_ID = FK_STORE_ID 
inner join product on product.PRODUCT_ID = FK_PRODUCT_ID group by STORE_ID, PRODUCT_ID;
select * from STOREANALYSIS_MV;
