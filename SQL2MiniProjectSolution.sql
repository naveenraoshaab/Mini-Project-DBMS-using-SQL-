create database sql2miniproject;
use sql2miniproject;


# 1. join all tables as a single table combined_table
create table combined_table as
(select m.cust_id,m.prod_id,m.ship_id,m.ord_id,m.sales,m.discount,
m.order_quantity,m.profit,m.shipping_cost,m.product_base_margin,
customer_name,province,region,customer_segment,od.order_id,order_date,
od.order_priority,product_category, product_sub_category,sd.ship_mode,
sd.ship_date
from market_fact m
left join cust_dimen c
on m.cust_id=c.cust_id
join orders_dimen od
on od.ord_id=m.ord_id
join prod_dimen pd
on m.prod_id=pd.prod_id
join shipping_dimen sd
on m.ship_id=sd.ship_id
order by customer_name);
select * from combined_table;



#2.Top 3 customers who have maximum number of orders
select cust_id,customer_name,count(cust_id) as number_of_orders
from combined_table
group by cust_id,customer_name
order by count(cust_id) desc
limit 3;



#3. Add a newcolumn daysTakenForDelivery
set sql_safe_updates=0;
update combined_table set order_date=str_to_date(order_date,'%d-%m-%Y');
alter table combined_table modify order_date date;
update combined_table set ship_date=str_to_date(ship_date,'%d-%m-%Y');
alter table combined_table modify ship_date date;
alter table combined_table add column daysTakenForDelivery int;
update combined_table set daysTakenForDelivery=datediff(ship_date,order_date);
select order_date,ship_date,daystakenfordelivery from combined_table;
select * from combined_table;



#4. Customer whose order took the maximum time to get delivered
select * from combined_table
where daystakenfordelivery=(select max(daystakenfordelivery) from combined_table);



#5. Retrieve total sales made by each product from the data (use Windows function)
select prod_id,
product_category,
product_sub_category,
sum(sales) over(partition by prod_id) Total_sales
from combined_table
order by total_sales desc;


#6. Retrieve total profit made from each product from the data (use windows function)
select prod_id,
product_category,
product_sub_category,
sum(profit) over(partition by prod_id) Total_profit
from combined_table
order by total_profit desc;



#7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
select count(distinct cust_id) as totalNumberofUniqueCustomers from combined_table
where monthname(order_date)='january';

select count(*) as `No. of customers who visited every month in 2011` from
(select cust_id,customer_name,count(distinct month) as no_of_distinct_months_visited from
(select cust_id,customer_name,monthname(order_date) as month
from combined_table
where year(order_date)=2011)t
group by cust_id,customer_name
having count(distinct month)=12)t2;


#8. Retrieve month-by-month customer retention rate since the start of the business.(using views)
create view visit as
(select customer_name,order_date,month(order_date) month
from combined_table
order by customer_name,order_date);

create view following as
(select *,lead(order_date) over() nextvisit from visit);

create view retention as
(select *,datediff(nextvisit,order_date) retention_value from following);

create view retentionfinal as
(select *,
(case
when retention_value<0 then null
when retention_value between 0 and 30 then 'retained'
when retention_value between 31 and 90 then 'irregular'
else 'churned'
end) retention_status
from retention);

select month,retention_status,
(count(retention_status)/(select count(retention_status) from retentionfinal where month=rf.month))*100  as retention_rate_percentage
from retentionfinal rf
where retention_status='retained'
group by month,retention_status
order by month;
