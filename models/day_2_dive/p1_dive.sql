with source as (
	
	select * from {{ ref('d2_p1') }} order by item

),

parsed as (
	select
		item,
		command,
		split(command, ' ')[offset(0)] as direction,
		cast(split(command, ' ')[offset(1)] AS INT64) as units
	from
		source

),

normalized as (
	select
		item,
		direction,
		case
			when direction = 'forward' then 'horizontal'
			when direction in ('up', 'down') then 'vertical'
			end as axis,
		if(direction = 'up', units * -1, units) as units
	from
		parsed
),

aggregated as (
	select
		axis,
		sum(units) as total_traveled
	from
		normalized
	group by
		axis
)

{#-
Of many ways to multiply these two values, this method uses
the log function to allow for a simple sum to derive the same
result.
#}
select
	round(exp(sum(ln(total_traveled)))) as solve
from 
	aggregated
