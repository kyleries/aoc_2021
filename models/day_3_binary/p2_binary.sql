declare O2_match_builder string DEFAULT "";
declare O2_break_condition default false;

declare CO2_match_builder string DEFAULT "";
declare CO2_break_condition default false;


set O2_match_builder = (
  select digit from (
  select 
    substr(binary, 0, 1) as digit, 
    count(substr(binary, 0, 1)) as freq
  from `strategic-reef-278304`.`aoc_2021`.`d3`
  group by digit
) 
order by freq desc limit 1);

LOOP  
set O2_match_builder = (
    select digit from (
      select 
        substr(binary, 0, length(O2_match_builder) + 1) as digit, 
        count(substr(binary, 0, length(O2_match_builder) + 1)) as freq
      from `strategic-reef-278304`.`aoc_2021`.`d3`
      where substr(binary, 0, length(O2_match_builder)) = O2_match_builder 
      group by digit
    ) 
    order by freq desc, digit desc limit 1
);
set O2_break_condition = (
  select
    case 
      when count(binary) = 1 then true
      when count(binary) > 1 then false
  end as rows_left
  from `strategic-reef-278304`.`aoc_2021`.`d3`
  where substr(binary, 0, length(O2_match_builder)) = O2_match_builder 
);
if O2_break_condition = true then 
set O2_match_builder = (
    select binary from `strategic-reef-278304`.`aoc_2021`.`d3` where substr(binary, 0, length(O2_match_builder)) = O2_match_builder 
);
break; 
end if;
end loop;
select O2_match_builder;

set CO2_match_builder = (
  select digit from (
  select 
    substr(binary, 0, 1) as digit, 
    count(substr(binary, 0, 1)) as freq
  from `strategic-reef-278304`.`aoc_2021`.`d3`
  group by digit
) 
order by freq limit 1);

loop
set CO2_match_builder = (
    select digit from (
      select 
        substr(binary, 0, length(CO2_match_builder) + 1) as digit, 
        count(substr(binary, 0, length(CO2_match_builder) + 1)) as freq
      from `strategic-reef-278304`.`aoc_2021`.`d3`
      where substr(binary, 0, length(CO2_match_builder)) = CO2_match_builder 
      group by digit
    ) 
    order by freq, digit limit 1
);
set CO2_break_condition = (
  select
    case 
      when count(binary) = 1 then true
      when count(binary) > 1 then false
  end as rows_left
  from `strategic-reef-278304`.`aoc_2021`.`d3`
  where substr(binary, 0, length(CO2_match_builder)) = CO2_match_builder 
);
if CO2_break_condition = true then 
set CO2_match_builder = (
    select binary from `strategic-reef-278304`.`aoc_2021`.`d3` where substr(binary, 0, length(CO2_match_builder)) = CO2_match_builder 
);
break; 
end if;
end loop;
select CO2_match_builder;

select 
(
	SELECT SUM(CAST(c AS INT64) << (LENGTH(O2_match_builder) - 1 - bit))
FROM UNNEST(SPLIT(O2_match_builder, '')) AS c WITH OFFSET bit)
 * 
(
	SELECT SUM(CAST(c AS INT64) << (LENGTH(CO2_match_builder) - 1 - bit))
FROM UNNEST(SPLIT(CO2_match_builder, '')) AS c WITH OFFSET bit)

as solve;
