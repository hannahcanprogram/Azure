--1. List of customers (name, city, id) and total quanitity of goods purchased on 2013/06/20 (check history)

select c.CustomerID, c.CustomerName, sum(ol.Quantity) as total_quantity_purchased
from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                         left join Sales.Customers for SYSTEM_TIME as of '2013-06-20' c on c.CustomerID = o.CustomerID
						 left join Application.Cities ct on ct.CityID = c.DeliveryCityID
group by c.CustomerID, c.CustomerName

--2. List of stockitems that ever had a raise of retial price (id, name, old retial price, new retail price, raise date) and the customers bought them the most (id, name). If a stockitem raised price more than once, only use the most recent one.

with cte as
(
select si.StockItemID, si.StockItemName, si.ValidFrom,si.RecommendedRetailPrice,
       isnull(lead(si.RecommendedRetailPrice,1) over(partition by si.StockItemID order by si.ValidFrom desc), si.RecommendedRetailPrice) as pre_price,
	   isnull(si.RecommendedRetailPrice-lead(si.RecommendedRetailPrice,1) over(partition by si.StockItemID order by si.ValidFrom desc),0) as change
from
  ( select * from Warehouse.StockItems FOR SYSTEM_TIME ALL) si
) 

select  StockItemID, StockItemName, 
        RecommendedRetailPrice as new_price,
		pre_price as old_price,
        max(ValidFrom) as raise_date
from
     cte
where change > 0 
group by StockItemID, StockItemName, RecommendedRetailPrice, pre_price
         
--3. List of StockItem Manufacturing countries and count of items made in that country.(use stockitems table)
select *
from
(
select  substring(si.CustomFields,Patindex('%": "%',si.CustomFields) + len('": "') ,
                   Patindex('%", "%',si.CustomFields) - (Patindex('%": "%',si.CustomFields) + len('": "'))   )
		as manufacturing_country,
        count(distinct si.StockItemID) as cnt_items
from  Warehouse.StockItems FOR SYSTEM_TIME ALL si 
group by substring(si.CustomFields,Patindex('%": "%',si.CustomFields) + len('": "') ,
                   Patindex('%", "%',si.CustomFields) - (Patindex('%": "%',si.CustomFields) + len('": "'))   )
) temp
where temp.manufacturing_country is not null
