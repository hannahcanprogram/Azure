
--1.All customers(names), and their postal cities, which brought more than 2000 "toys" (as stock group) in 2016
select * from Sales.Customers
select * from Sales.OrderLines
select * from Sales.Orders
select * from Application.Cities
select * from Warehouse.StockGroups
select * from Warehouse.StockItemStockGroups

with cte1 as
(
select si.StockItemID
from Warehouse.StockItems si left join Warehouse.StockItemStockGroups sig on si.StockItemID = sig.StockItemID
                            left join Warehouse.StockGroups sg on si.StockItemID = sg.StockGroupID
                            and sg.StockGroupName = 'Toys'
)
 
select temp.CustomerName, ct.CityName
from 
(
select c.CustomerID, c.CustomerName, c.DeliveryCityID, sum(ol.Quantity) as quantity
from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                         left join cte1 on ol.StockItemID = cte1.StockItemID
						 right join Sales.Customers c on c.CustomerID = o.CustomerID
						 and datepart(year,o.OrderDate)= 2016
group by c.CustomerID, c.CustomerName , c.DeliveryCityID
having sum(ol.Quantity) > 2000
) temp 
       left join Application.Cities ct 
	   on temp.DeliveryCityID = ct.CityID

--2.
select * from Sales.OrderLines
select * from Sales.Orders
select * from Purchasing.PurchaseOrderLines
select * from Purchasing.PurchaseOrders
select * from Warehouse.StockItems

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
		                and pur_temp.pur_quantity > sal_temp.sal_quantity


--3.

select * from Sales.SpecialDeals
select * from Warehouse.StockItemStockGroups
select * from Sales.Orders
Select * from Sales.OrderLines
Select * from Warehouse.StockItems
Select * from Warehouse.StockGroups
select * from Sales.Customers
select * from Sales.CustomerCategories




with cte1 as 
(
select sig.StockItemID as disc_stockItemID
from Warehouse.StockItemStockGroups sig 
where sig.StockGroupID in (select distinct sd.StockGroupID from Sales.SpecialDeals sd)
)

select ol.OrderLineID, ol.OrderID, ol.StockItemID, ol.Quantity, ol.UnitPrice, o.OrderDate, c.BuyingGroupID
       case when ol.StockItemID in (select disc_stockItemID from cte1)
	           and c.BuyingGroupID in (select sd.BuyingGroupID from Sales.SpecialDeals sd)
			then 
from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                         left join Sales.Customers c on o.CustomerID = c.CustomerID 
						 left join Sales.SpecialDeals sd on c.BuyingGroupID = sd.BuyingGroupID


