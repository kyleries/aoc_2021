with source as (

	  select * from {{ ref('d1_p1') }} order by item

),

final as (

	  select
	    if(
		(lag(sonar_read) over (order by item) < sonar_read), 1, 0
	    ) as increase
	  from
	    source

)

select sum(increase) as day_1_answer from final
