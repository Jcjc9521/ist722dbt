with stg_orders as 
(
    select
        OrderId,  
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        OrderId,
        {{ dbt_utils.generate_surrogate_key(['Productid']) }} as productkey, 
        sum(Quantity) as quantity, 
        sum(Quantity*UnitPrice) as extendedpriceamount,
        sum(Quantity * UnitPrice * Discount) as discountamount,
        sum((quantity * UnitPrice)-(Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by OrderId, Productid
)

select  
    o.OrderId,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    od.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_orders o
join stg_order_details od on o.OrderId = od.OrderId

