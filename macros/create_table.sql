{% macro create_table() %}

    {% set sql %}
        drop table if exists dbt.task1;

        create table dbt.task1 (week_num char(10), status char(35), count_num int);

        insert into dbt.task1
        select to_char(order_date, 'WW') as week_num, status, count(*) as count_num
        from dbt.hub_order a
        join dbt.sat_order_details b on a.order_pk = b.order_pk 
        group by week_num, status;

        drop table if exists dbt.task2;

        create table dbt.task2 (customer_key char(10), last_name char(45), first_name char(45), email char(45));

        insert into dbt.task2
        select customer_key, last_name, first_name, email
        from (
        select 
            a.customer_key, 
            b.last_name, 
            b.first_name, 
            b.email, 
            row_number() over(partition by a.customer_key order by b.effective_from desc) as r_num
        from dbt.hub_customer a
        inner join dbt.sat_customer_details b on a.customer_pk = b.customer_pk and now() >= b.effective_from
        where a.customer_key in ('100', '101')) as t
        where r_num = 1
    {% endset %}

    {% for query in sql.split(';') if query.strip() %}
        {% do run_query(query) %}
    {% endfor %}

{% endmacro %}