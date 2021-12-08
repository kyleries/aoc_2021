with source as (

	select * from {{ ref('d2_p1') }} order by item

),

prepared as
  (SELECT item,
          command,
          split(command, ' ')[offset(0)] as direction,
          cast(split(command, ' ')[offset(1)] AS INT64) as units
   FROM source
   order by item),

aim_adjustments as
  (select *,
          case
              when direction = 'down' then units
              when direction = 'up' then units * -1
              else 0
          end as aim_adjust
   from prepared),

set_current_aim as
  (select *,
          sum(aim_adjust) over (
                                order by item) as current_aim
   from aim_adjustments),

depth_adjustments as
  (select *,
          case
              when direction = 'forward' then units * current_aim
              else 0
          end as depth_adjust
   from set_current_aim),

set_current_depth as
  (select *,
          sum(depth_adjust) over (
                                  order by item) as current_depth
   from depth_adjustments),

forward_adjustments as
  (select *,
          case
              when direction = 'forward' then units
              else 0
          end as forward_movement
   from set_current_depth),

set_forward_movement as
  (select *,
          sum(forward_movement) over (
                                      order by item) as current_forward_location
   from forward_adjustments),
final_solution as
  (select 	last_value(current_depth) over (order by item ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * 
		last_value(current_forward_location) over (order by item ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as solution
   from set_forward_movement)

select *
from final_solution
limit 1
