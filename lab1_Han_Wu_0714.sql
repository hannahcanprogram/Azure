
--1.All customers(names), and their postal cities, which brought more than 2000 "toys" (as stock group) in 2016

/*
select * from Sales.Customers
select * from Sales.OrderLines
select * from Sales.Orders
select * from Application.Cities
select * from Warehouse.StockGroups
select * from Warehouse.StockItemStockGroups
*/

with cte1 as
(
select si.StockItemID
from Warehouse.StockItems si left join Warehouse.StockItemStockGroups sig on si.StockItemID = sig.StockItemID
                            left join Warehouse.StockGroups sg on si.StockItemID = sg.StockGroupID
where sg.StockGroupName = 'Toys'
)
 
select temp.CustomerName, temp.CityName
from
(
select c.CustomerID, c.CustomerName, ct.CityName, sum(ol.Quantity) as quantity
from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                         left join cte1 on ol.StockItemID = cte1.StockItemID
						 right join Sales.Customers c on c.CustomerID = o.CustomerID
						 right join Application.Cities ct on ct.CityID = c.PostalCityID
where datepart(year,o.OrderDate)= 2016
group by c.CustomerID, c.CustomerName , ct.CityName
having sum(ol.Quantity) > 2000
) temp

--2.
/*
select * from Sales.OrderLines
select * from Sales.Orders
select * from Purchasing.PurchaseOrderLines
select * from Purchasing.PurchaseOrders
select * from Warehouse.StockItems
*/

select si.StockItemName
from
Warehouse.StockItems si left join
                                (
                                 select pol.StockItemID, 
								        sum(pol.OrderedOuters * si.QuantityPerOuter) as pur_quantity
                                 from Purchasing.PurchaseOrderLines pol 
								        left join Purchasing.PurchaseOrders po 
											 on pol.PurchaseOrderID = po.PurchaseOrderID
                                            and datepart(year,po.OrderDate) = 2016
									    left join Warehouse.StockItems si 
										     on si.StockItemID = pol.StockItemID
                                 group by pol.StockItemID
                               ) pur_temp 
                           on si.StockItemID = pur_temp.StockItemID
                         left join
                                 (
                                 select ol.StockItemID, 
								        sum(Quantity) as sal_quantity
                                  from Sales.OrderLines ol 
								         left join Sales.Orders o 
										 on ol.OrderID = o.OrderID
                                         and datepart(year,o.OrderDate) = 2016
                                   group by ol.StockItemID
                                  ) sal_temp 
                            on si.StockItemID = sal_temp.StockItemID 
where pur_temp.pur_quantity > sal_temp.sal_quantity


--3.
/*
select * from Sales.SpecialDeals
select * from Warehouse.StockItemStockGroups
select * from Sales.Orders
Select * from Sales.OrderLines
Select * from Warehouse.StockItems
Select * from Warehouse.StockGroups
select * from Sales.Customers
select * from Sales.CustomerCategories
select * from Sales.BuyingGroups
*/

with cte1 as 
(
select sig.StockItemID
from Warehouse.StockItemStockGroups sig 
where sig.StockGroupID in (select distinct sd.StockGroupID from Sales.SpecialDeals sd)
)

select sum( ol.Quantity*ol.UnitPrice*sd.DiscountPercentage)  as loss_profit

from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                         left join Sales.Customers c on o.CustomerID = c.CustomerID 
						 join warehouse.StockItemStockGroups ssg on ssg.StockItemID = ol.StockItemID
						 join Sales.SpecialDeals sd on sd.BuyingGroupID = c.BuyingGroupID and sd.StockGroupID = ssg.StockGroupID
where c.CustomerID in (
          select c.CustomerID
		  from Sales.Customers c 
		  where c.BuyingGroupID in (select * from (select sd.BuyingGroupID as disc_buyinggoupID
                                    from  Sales.SpecialDeals sd  ) temp)  )
	  and 
	  ol.StockItemID in ( select * from cte1 ) 
	  and
	  o.OrderDate between sd.StartDate and sd.EndDate
	      





