with raw as (
	    
	    SELECT item, split(cast(binary as string), "") as dat FROM {{ ref('d3') }}

),

rowed as (
	    select item, bit, pos from raw, raw.dat as bit with offset as pos
),

grouped as (

	    select
	        pos,
		bit,
		count(bit) as num_bits
	    from
	        rowed
	    group by 
	        pos, bit
	),

binaries as (

    select 
        distinct pos, 
        first_value(bit) over (partition by pos order by num_bits desc) as g,
        first_value(bit) over (partition by pos order by num_bits asc) as e
    from grouped 
    order by pos

),

combinaries as (

	select
		string_agg(g, '' order by pos) as g,
		string_agg(e, '' order by pos) as e
	from
		binaries

)

select 
    {{ from_binary('g')}} * {{ from_binary('e') }}  as solve
from 
    combinaries

