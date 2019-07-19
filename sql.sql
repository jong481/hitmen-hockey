select * from wnba_game_stats_four_factors 
select * from wnba_game_schedule
SELECT count(gid) FROM wnba_game_schedule where cast(gdte as date) < now() - interval '1 day'

select max(cast(a.gdte as date)) from (
	select distinct s.game_id,
		   f.gdte
	from wnba_game_stats_four_factors s
	left join wnba_game_schedule f on s.game_id = f.gid	
) a

				
				
select distinct s.game_id,
	   cast(f.gdte as date)
from wnba_game_stats_four_factors s
left join wnba_game_schedule f on s.game_id = f.gid	
order by cast(f.gdte as date) desc;
				
select * from wnba_game_stats_four_factors where game_id = '1021900060'