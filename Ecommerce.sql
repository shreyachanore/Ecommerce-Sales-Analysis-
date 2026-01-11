-- DATABASE SECTION

create database ecommercedb;
use ecommercedb;
select * from customers;
select * from order_items;
select * from orders;
select * from products;
select * from returns;

--  (SECTION 2 )PREVIEW FIRST5
select * from customers limit 5;
select * from order_items limit 5;
select * from orders limit 5;
select * from products limit 5;
select * from returns limit 5;

-- DATA COUNTS
select 'customer' as table_name,count(*)as myrows from customers 
union all
select 'order',count(*)from orders 
union all
select 'order_items',count(*)from order_items 
union all
select 'products',count(*)from products 
union all
select 'reurns',count(*)from returns ;

-- SECTION 3
-- DATA QUALITY -NULL AND MISSING VALUES CHECKS
-- COUNTS NULL IN CUSTOMERS
select
sum(customer_id is null) as nulls_customer_id,
sum(name is null) as nulls_name,
sum(email is null) as nulls_email,
sum(signup_date is null)as nulls_signup_date,
sum(region is null) as nulls_regions
from customers;
-- count of null in orders
select
sum(order_id is null) as nulls_order_id,
sum(customer_id is null) as nulls_customer_id,
sum(order_date is null) as nulls_order_date,
sum(total_amount is null)as nulls_total_amount
from orders;
-- count nulls in order_items
select
sum(order_item_id is null) as nulls_order_item_id,
sum(order_id is null) as nulls_order_id,
sum(product_id is null) as nulls_product_id,
sum(quantity is null)as nulls_quantity,
sum(item_price is null) as nulls_item_price
from order_items;
-- count nulls in products
select
sum(product_id is null) as nulls_product_id,
sum(name is null) as nulls_name,
sum(category is null) as nulls_category,
sum(price is null)as nulls_price
from products;
-- count nulls in return
select
sum(return_id is null) as null_return_id,
sum(order_id is null)as nulls_order_id,
sum(return_date is null) as nulls_return_date,
sum(reason is null) as nulls_reason
from returns;

-- percent nulls by column in customers
-- if i want to see in percentage null values
select
100* sum(name is null)/count(*)as pct_null_name,
100* sum(email is null)/count(*)as pct_null_email,
100* sum(signup_date is null)/count(*)as pct_null_signup_date,
100* sum(region is null)/count(*)as pct_null_region
from customers;

-- SECTION 4 BASICS STATISTICS AND SUMMARY METRICS
-- basis ststiv for order amont
select
min(total_amount) as min_amt,
max(total_amount)as max_amt,
avg(total_amount)as avg_amt,
sum(total_amount)as sum_amt
from orders;
-- basics statistics for order item price and quantities
select
min(item_price)as min_price,
max(item_price)as max_price,
avg(item_price)as avg_price,
min(quantity)as min_qty,
max(quantity)as max_qty,
avg(quantity)as avg_qty
from order_items;

-- SECTION 5 DUPLICATE DATA HANDLING
-- find customers with dulplicate emails
select email,count(*)as dup_count
from customers 
group by email
having count(*)>1;

-- find dulpicate orders by customers and date
select customer_ID,order_date,count(*)as dup_count
from orders
group by customer_id,order_date
having count(*)>1;

-- find duplicate order_item for same product in same order
select order_id,product_id,count(*)as dup_count
from order_items
group by order_id,product_id
having count(*)>1;

-- disable safe mode for deletion operation
set sql_safe_updates=0;

-- delete duplicate customers by email(keep earliest signup)
with ranked as (
select *,row_number()over(partition by email order by signup_date)as rn from customers
)
delete from customers
where customer_id in (select customer_id from ranked where rn>1); 

-- (common table expression) delete duplicate order_item by order-product combination
with ranked as (
select order_item_id,row_number() over(partition by order_id,product_id order by order_item_id)as rn
from order_items
)
delete from order_items
where order_item_id in (select order_item_id from ranked where rn>1); 

-- SECTION 6  DATA VALIDATION AND CLEANING PATTERNS

-- find blank or invalid email addresses
select * from customers
where email is null
or trim(email)=''
or email not regexp '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';

-- find products with missing names
select* from products where name is null or trim(name)='';

-- region imputed
-- null ko replace kar rhe hai
select customer_id,name,email,signup_date,
coalesce(region,'unknown')as region_imputed
from customers;

-- drop rows with null total_amount
select * from orders where total_amount is not null;

-- section 7 REFERNTIAL INTEGRITY checks
-- print kar rhe hai jiski id null hai
select o.* from orders o 
left join customers c on c.customer_id=o.customer_id
where c.customer_id is null;

-- order items referencing missing products
select oi.*
from order_items oi
left join products p on p.product_id=oi.product_id
where p.product_id is null;

-- returns referncing missing orders
select r.* from returns r
left join orders o on o.order_id=r.order_id
where o.order_id is null;


-- SECTION 8 SALES REVENUE ANALYSIS 
-- REVENUE BY PRODUCT CATEGORY
select p.category,sum(oi.quantity*oi.item_price)as revenue
from order_items oi
join products p on p.product_id=oi.product_id
group by p.category
order by revenue desc;

-- top5 products by revenue
select p.product_id,p.name,sum(oi.quantity * oi.item_price)as revenue
from order_items oi
join products p on p.product_id=oi.product_id
group by p.product_id,p.name
order by revenue desc
limit 5;

-- orders per customer
select c.customer_id ,c.name,count(o.order_id)as orders_count
from customers c
left join orders o on o.customer_id=c.customer_id
group by c.customer_id,c.name
order by orders_count desc;

-- average order value (avo)
select avg(total_amount) as avg_order_value from orders;

-- customer-level total revenue
select c.customer_id,c.name,sum(oi.quantity * oi.item_price)as total_revenue from customers c
join orders o on o.customer_id=c.customer_id
join order_items oi on oi.order_id=o.order_id 
group by c.customer_id,c.name
order by total_revenue desc;

--  SECTION 9 timee based analysis
-- monthly revenue trend
select date_format(o.order_date,'%Y-%m') as month,
sum(oi.quantity * oi.item_price)as revenue 
from orders o
join order_items oi on oi.order_id=o.order_id
group by month
order by month;

-- daily order counts
select date(order_date)as order_day,count(*)as orders
from orders
group  by order_day
order by order_day;

-- SECTION 10: PRODUCT AND CATEGORY ANALYSIS
-- category mix by month
select date_format(o.order_date,'%Y-%m')as month,p.category,
sum(oi.quantity)as units_sold
from orders o
join order_items  oi on oi.order_id=o.order_id
join products p on p.product_id=oi.product_id
group by month,p.category
order by month,units_sold desc;

-- first order date per customer
select customer_id,min(order_date)as first_order_date
from orders 
group by customer_id;

-- SECTION11 CUSTOMER BEHAVIOR
select date_format(signup_date,'%Y-%m')as signup_month,count(*)as new_customers
from customers
group by signup_month
order by signup_month;

-- orders placed within 30 days of signup(early activation)
select c.customer_id,count(o.order_id)as orders_in_30d
from customers c
left join orders o 
on o.customer_id=c.customer_id
and o.order_date<=date_add(c.signup_date,interval 30 day)
group by c.customer_id
order by orders_in_30d desc;
-- SECTION 12 WINDOW FUNCTION AND ANALYTICS
 -- rank customers by total revenue
 select customer_id,name,total_revenue,
 dense_rank() over(order by total_revenue desc)as rank_position
 from (
 select c.customer_id,c.name,sum(oi.quantity*oi.item_price)as total_revenue
 from customers c
 join orders o on o.customer_id=c.customer_id
 join order_items oi on oi.order_id=o.order_id
 group by c.customer_id,c.name
 )as t;
 
-- SECTION 13 RETURNS AND REFUND ANALYSIS
-- return rate(% of total orders)
select 100 * count(distinct r.order_id)/count(distinct o.order_id)as return_rate_pct
from orders o
left join returns r on r.order_id=o.order_id;

-- return  reasons
select reason,count(*) as reason_count
from returns
group by reason
order by reason_count desc;

-- revenus loss to returns (assuming full refund)
select sum(o.total_amount)as refund_value
from orders o
join returns r on r.order_id= o.order_id;


-- SECTION 14 
-- GEOGRAPHICAL INSIGHTS
select region,count(*)as customer_count
from customers 
group by region 
order by customer_count desc;

-- revenue by region 
select c.region,sum(oi.quantity*oi.item_price)as regional_revenue
from customers c
join orders o on c.customer_id=c.customer_id
join order_items oi on oi.order_id=o.order_id
group by c.region
order by regional_revenue desc;