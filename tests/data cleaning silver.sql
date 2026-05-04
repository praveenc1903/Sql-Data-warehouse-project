--check nulls and duplicates in primary key 

select cst_id,count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

select * from 
(select *,
row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)t
where flag_last = 1; 

--check unwanted spaces 

select cst_firstname 
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname 
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);



select 
cst_id,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date,
flag_last
from 
(select *,
row_number() over( partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)t
where flag_last = 1; 

--Data coonsistency and standardization 

select distinct cst_gndr
from bronze.crm_cust_info;