
--1.
with cte as
(select s.PrimaryContactPersonID as id, s.PhoneNumber, s.FaxNumber
from Purchasing.Suppliers s
where s.PrimaryContactPersonID is not null
union
select s.AlternateContactPersonID as id, s.PhoneNumber, s.FaxNumber
from Purchasing.Suppliers s
where s.AlternateContactPersonID is not null
union
select c.PrimaryContactPersonID as id, c.PhoneNumber, c.FaxNumber
from Sales.Customers c
where c.PrimaryContactPersonID is not null
union
select c.AlternateContactPersonID as id, c.PhoneNumber, c.FaxNumber
from Sales.Customers c
where c.AlternateContactPersonID is not null
)

select p.FullName, p.PhoneNumber, p.FaxNumber, cte.PhoneNumber as CompanyPhoneNumber, cte.FaxNumber as CompanyFaxNumber
from Application.People p left join cte on p.PersonID = cte.id


--2.
select c.CustomerName
from Sales.Customers c left join Application.People p on c.PrimaryContactPersonID = p.PersonID
where c.PhoneNumber = p.PhoneNumber 


--3.
select c.CustomerName
from Sales.Customers c
where c.CustomerID not in
                (
                 select o.CustomerID
			     from Sales.Orders o 
			     where o.OrderDate >= cast('2016-01-01' as date)
			      ) 

/* check customerIDs who made orders after 2016-01-01, got all customerIDs
select o.CustomerID, min(o.OrderDate)
from Sales.Orders o 
where o.OrderDate >= cast('2016-01-01' as date)
group by o.CustomerID
order by o.CustomerID
*/


--4.
select temp.StockItemID, s.StockItemName, temp.OrderedOuters * s.QuantityPerOuter as TotalQuantity
from
(
select ol.StockItemID, sum(ol.OrderedOuters) as OrderedOuters 
from Purchasing.PurchaseOrderLines ol join Purchasing .PurchaseOrders o on ol.PurchaseOrderID = o.PurchaseOrderID
where datepart(year, o.OrderDate) = 2013
group by ol.StockItemID
) temp 
      left join Warehouse.StockItems s 
             on temp.StockItemID = s.StockItemID
order by TotalQuantity desc



--5.
select s.StockItemName
from Warehouse.StockItems s left join Purchasing.PurchaseOrderLines ol 
                                   on s.StockItemID = ol.StockItemID
where len(ol.Description) >= 10


--6.
select distinct s.StockItemName
from Warehouse.StockItems s left join Sales.OrderLines ol on s.StockItemID = ol.StockItemID
                            left join Sales.Orders o on ol.OrderID = o.OrderID
							left join Sales.Customers c on o.CustomerID = c.CustomerID
							left join Application.Cities ct on ct.CityID = c.DeliveryCityID
							left join Application.StateProvinces sp on sp.StateProvinceID = ct.StateProvinceID
where datepart(year, o.OrderDate) = 2015 and
      sp.StateProvinceName not in('Alabama','Geogia')



--7.
select sp.StateProvinceName, temp.avg_ProcessDates
from
(
select ct.StateProvinceID, avg(datediff(day,o.OrderDate,i.ConfirmedDeliveryTime))as avg_ProcessDates
from Sales.Orders o left join Sales.Invoices i on o.OrderID = i.OrderID
                    left join Sales.Customers c on o.CustomerID = c.CustomerID
					left join Application.Cities ct on ct.CityID = c.DeliveryCityID
group by ct.StateProvinceID
) temp
	  right join Application.StateProvinces sp on sp.StateProvinceID = temp.StateProvinceID
 


--8.
select sp.StateProvinceName, temp.avg_ProcessDates
from
(
select ct.StateProvinceID, avg(datediff(day,o.OrderDate,i.ConfirmedDeliveryTime))as avg_ProcessDates
from Sales.Orders o left join Sales.Invoices i on o.OrderID = i.OrderID
                    left join Sales.Customers c on o.CustomerID = c.CustomerID
					left join Application.Cities ct on ct.CityID = c.DeliveryCityID
group by ct.StateProvinceID, datepart(year, o.OrderDate), datepart(month, o.OrderDate)
) temp
      right join Application.StateProvinces sp on sp.StateProvinceID = temp.StateProvinceID



--9.
with temp_pur as
  (
    select pol.StockItemID, s.StockItemName, sum(pol.OrderedOuters * s.QuantityPerOuter) as purchasequantity
    from Purchasing.PurchaseOrderLines pol left join Purchasing.PurchaseOrders po on pol.PurchaseOrderID = po.PurchaseOrderID
                                       left join Warehouse.StockItems s on pol.StockItemID = s.StockItemID
    where datepart(year, po.OrderDate) = 2015
     group by pol.StockItemID, s.StockItemName
   ) 
, temp_or as 
   (
    select ol.StockItemID, s.StockItemName, sum(ol.Quantity) as orderquantity
    from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID
                             left join Warehouse.StockItems s on ol.StockItemID = s.StockItemID
    where datepart(year,o.OrderDate) = 2015
    group by ol.StockItemID, s.StockItemName
    )

select distinct temp_pur.StockItemName
from temp_pur full join temp_or 
               on temp_pur.StockItemID = temp_or.StockItemID
where temp_pur.purchasequantity > temp_or.orderquantity
union
select distinct temp_or.StockItemName
from temp_pur full join temp_or 
               on temp_pur.StockItemID = temp_or.StockItemID
where temp_pur.purchasequantity > temp_or.orderquantity



--10.
with cte as (
select o.CustomerID, sum(temp1.mug_order_quantity) as mug_cus_quantity
from
(
select ol.OrderID, sum(ol.Quantity) as mug_order_quantity
from Sales.OrderLines ol 
where ol.Description like'%mug%'
group by ol.OrderID
) temp1 
       right join Sales.Orders o on temp1.OrderID = o.OrderID
where datepart(year,o.OrderDate)=2016
group by o.CustomerID
)
select c.CustomerName, c.PhoneNumber, p.FullName as PrimaryContactPersonName, cte.mug_cus_quantity
from Sales.Customers c
      left join Application.People p on c.PrimaryContactPersonID = p.PersonID
	  left join cte on c.CustomerID = cte.CustomerID
where cte.mug_cus_quantity < 10



--11.
select ct.CityName
from
(
select distinct s.DeliveryCityID
from Purchasing.Suppliers s
where datepart(year, s.ValidFrom) >= 2015
union
select distinct s.DeliveryCityID
from Sales.Customers s
where datepart(year, s.AccountOpenedDate) >= 2015
)temp join Application.Cities ct on ct.CityID = temp.DeliveryCityID



--12.
select i.StockItemName,
       CONCAT(c.DeliveryAddressLine1,c.DeliveryAddressLine2) as deliveryaddress,
	   sp.StateProvinceName, 
	   ct.CityName,
	   ctr.CountryName,
       c.CustomerName,
	   p1.FullName as primarycontactperson, 
	   p2.FullName as altercontactperson, 
       c.PhoneNumber as customerphone,
	   temp.quantity as quantity
 from
(
select ol.OrderID, ol.StockItemID, Sum(Quantity) as quantity
from Sales.OrderLines ol left join Sales.Orders o on ol.OrderID = o.OrderID and o.OrderDate = cast('2014-07-01' as date)
group by ol.OrderID, ol.StockItemID
)temp   
     left join Warehouse.StockItems i on temp.StockItemID = i.StockItemID
     left join Sales.Orders o on o.OrderID = temp.OrderID 
	 left join Sales.Customers c on c.CustomerID = o.CustomerID
	 left join Application.People p1 on p1.PersonID = c.PrimaryContactPersonID
	 left join Application.People p2 on p2.PersonID = c.AlternateContactPersonID
     left join Application.Cities ct on ct.CityID = c.DeliveryCityID
     left join Application.StateProvinces sp on sp.StateProvinceID = ct.StateProvinceID
	 left join Application.Countries ctr on ctr.CountryID = sp.CountryID



--13.

--select * from Sales.SpecialDeals
--select * from Warehouse.StockGroups
--select * from Warehouse.StockItemStockGroups
--select * from Warehouse.StockItems
--select * from Purchasing.PurchaseOrderLines
--select * from Purchasing.PurchaseOrders
--select * from Sales.OrderLines
--select * from Sales.Orders

select p.StockGroupName, p.total_purchase, s.total_sale,
       p.total_purchase - s.total_sale as remaining_stock
from
(
select sg.StockGroupName, 
        sum(pol.OrderedOuters * i.QuantityPerOuter) as total_purchase
from Purchasing.PurchaseOrderLines pol left join Warehouse.StockItems i on i.StockItemID = pol.StockItemID
                                       join Warehouse.StockItemStockGroups ssg on ssg.StockItemID = pol.StockItemID
									   join Warehouse.StockGroups sg on sg.StockGroupID = ssg.StockGroupID
group by sg.StockGroupName
) p join
(
select sg.StockGroupName, 
        sum(ol.Quantity) as total_sale
from Sales.OrderLines ol left join Warehouse.StockItemStockGroups ssg on ssg.StockItemID = ol.StockItemID
									   join Warehouse.StockGroups sg on sg.StockGroupID = ssg.StockGroupID
group by sg.StockGroupName
) s on p.StockGroupName= s.StockGroupName


--14.
with cte as (
select temp2.CityID, temp2.CityName,temp2.StockItemID, temp2.StockItemName,temp2.received_quantity,ctr.CountryName
from
(
     select temp.CityID, temp.CityName,temp.StockItemID, temp.StockItemName,temp.received_quantity
       from
           (
            select ct.CityID, ct.CityName,ol.StockItemID, i.StockItemName, 
                   sum(ol.Quantity) as received_quantity,
	               ROW_NUMBER() over (partition by ct.CityName order by sum(ol.Quantity) desc) as ranking
              from Sales.OrderLines ol 
                           left join Warehouse.StockItems i on ol.StockItemID = i.StockItemID
			               left join Sales.Orders o on o.OrderID = ol.OrderID
			               left join Sales.Customers c on o.CustomerID= c.CustomerID  
			               left join Application.Cities ct on c.DeliveryCityID =ct.CityID
              where datepart(year,o.ExpectedDeliveryDate) = 2016 
           group by ct.CityID, ct.CityName, ol.StockItemID, i.StockItemName
            ) temp
      where temp.ranking = 1
) temp2 	
      left join Application.Cities c on temp2.CityID = c.CityID
      left join Application.StateProvinces sp on sp.StateProvinceID = c.StateProvinceID
	  left join Application.Countries ctr on sp.CountryID = ctr.CountryID 
where ctr.CountryName = 'United States'
)

select temp_cities.CityName, temp_cities.StateProvinceName, temp_cities.CountryName,
       isnull(cte.StockItemName,'No Sales') as MostDeliveriesStockItem
from
(
select c.CityID, c.CityName,sp.StateProvinceName, ctr.CountryName
from Application.Cities c
          left join Application.StateProvinces sp on sp.StateProvinceID = c.StateProvinceID
		  left join Application.Countries ctr on sp.CountryID = ctr.CountryID 
	where ctr.CountryName = 'United States'
)temp_cities left join cte on temp_cities.CityID = cte.CityID

--15.
select distinct(inv.OrderID)
from Sales.Invoices inv
where inv.ReturnedDeliveryData like '%Receiver not present%'

--16.
select distinct si.StockItemID, si.StockItemName
from warehouse.StockItems for system_time all si
where si.CustomFields like '%China%'

--17.
select sti.manufacturing_country,
       sum(ol.Quantity) as total_quantity
from Sales.OrderLines ol left join 
(
select  *,
        substring(si.CustomFields,Patindex('%": "%',si.CustomFields) + len('": "') ,
                   Patindex('%", "%',si.CustomFields) - (Patindex('%": "%',si.CustomFields) + len('": "'))   )
		as manufacturing_country
from  Warehouse.StockItems si 
) sti on ol.StockItemID = sti.StockItemID
                           left join Sales.Orders o on o.OrderID = ol.OrderID
where year(o.OrderDate) = 2015
group by sti.manufacturing_country

--18.
create view stockgoups
as
select StockGroupName,[2013],[2014],[2015],[2016],[2017]
from
(
select sg.StockGroupName as StockGroupName, year(o.OrderDate) as years, sum(ol.Quantity) as total_quantity
from Sales.OrderLines ol join sales.Orders o on ol.OrderID = o.OrderID
                         join Warehouse.StockItems i on ol.StockItemID = i.StockItemID
						 join Warehouse.StockItemStockGroups ssg on ssg.StockItemID = ol.StockItemID
						 join Warehouse.StockGroups sg on sg.StockGroupID = ssg.StockGroupID
where year(o.OrderDate) in ( 2013, 2014, 2015, 2016, 2017)
group by sg.StockGroupName, year(o.OrderDate)
) p
pivot
(
sum(p.total_quantity) 
for years in
([2013],[2014],[2015],[2016],[2017]) 
)as pivotTable

--19.

select years, 'Clothing', 'USB Novelties', 'Computing Noveilties','Airline Novelties', 'Novelty Items','T-Shirts', 'Mugs','Furry Footwear','Toys', 'Packaging Materials'
from
(
select year(o.OrderDate) as years, sg.StockGroupName as StockGroupName, sum(ol.Quantity) as total_quantity
from Sales.OrderLines ol join sales.Orders o on ol.OrderID = o.OrderID
                         join Warehouse.StockItems i on ol.StockItemID = i.StockItemID
						 join Warehouse.StockItemStockGroups ssg on ssg.StockItemID = ol.StockItemID
						 join Warehouse.StockGroups sg on sg.StockGroupID = ssg.StockGroupID
where year(o.OrderDate) in ( 2013, 2014, 2015, 2016, 2017)
group by sg.StockGroupName, year(o.OrderDate)
) p
pivot
(
sum(p.total_quantity)
for StockGroupName in ('Clothing', 'USB Novelties', 'Computing Noveilties','Airline Novelties', 'Novelty Items','T-Shirts', 'Mugs','Furry Footwear','Toys', 'Packaging Materials')
) as pivotTable2


--20.
create function invoicetotal(@invoiceId int)
returns table
as
return
(
    select p.total_quantity
	from
	(
	select i.InvoiceID, sum(il.Quantity * il.UnitPrice) as total_quantity
    from  Sales.Invoices i left join Sales.InvoiceLines il on il.InvoiceID = i.InvoiceID 
	    --input is invoice id so use left join to include all invoice id
    where i.InvoiceID = @invoiceId
    group by i.InvoiceID
	) p
);

select * , 
      ( select * from invoicetotal(i.InvoiceID)) order_total
	from Sales.Invoices i


--21.

