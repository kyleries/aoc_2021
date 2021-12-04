with source as (

	  select * from {{ ref('d1_p2') }} order by item

),

final as (

	  select
	    item,
	    sonar_read,
	    sum(sonar_read) over (order by item rows between 3 preceding and 1 preceding) as prior_read,
	    sum(sonar_read) over (order by item rows between 2 preceding and current row) as current_read,
	    if(
		(   sum(sonar_read) over (order by item rows between 3 preceding and 1 preceding) <
		    sum(sonar_read) over (order by item rows between 2 preceding and current row)
		    and count(sonar_read) over (order by item rows between 3 preceding and 1 preceding) = 3
		    and count(sonar_read) over (order by item rows between 2 preceding and current row) = 3
		 ), 1, 0
	    ) as increase
	  from
	    source

)

select sum(increase) as day_1_p2_answer from final
