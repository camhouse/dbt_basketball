with top_asts as (
	select year, player, pos, tm, g, pts, ast, ft, row_number() over(partition by year order by ast desc) ast_rank
	from {{ ref('stg_season_stats') }}
)
select t.*, b.top_rated_points 
from top_asts t 
left join (select player, min(pts_rank)  top_rated_points from {{ref('top_points_per_year')}} group by 1) b on b.player = t.player
where ast_rank <= 3
