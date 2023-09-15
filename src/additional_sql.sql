--CUSTOMER	table			
--customer_key	Last_Name	Adress_City	Adress_State	event_time
					
--order_details table
--order_id customer_id event_time total_amount order_count order_details

--siteVisit table
--site_viit_id event_time customer_id


--Above tables we can create in aws postgress aurora table for better performance 
-- Below is query which returns maximum customer id in ascending order (Assume we ingest this data to respective table using python)

with site_visit as (select customer_id,count(*) as customer_visit_count 
								from siteVisit group by customer_id),

weeks as (select customer_key as customer_id,
				case when DATEDIFF(week, order_date,joinDate)<1 
					then 1
				else  DATEDIFF(week, order_date,joinDate)
				END as week ,
				sv.customer_visit_count
		from (select cs.customer_key, 
						cs.event_time as joinDate , 
						max(od.event_time) as order_date
				from  customer cs 
				join  order_details  od
						on cs.customer_key = od.customer_id) a
			join site_visit sv
				on sv.customer_id=a.customer_key ),

calculate_lvt as (select  customer_id,(52 * (orderedAmount / week )) * 10 as lvt from weeks )

select calculate_lvt from calculate_lvt order by lvt asc



					
					
					
					
					
					
					
					
					
