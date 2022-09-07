with top_pts as (
	select year, player, pos, tm, g, pts, ast, ft, row_number() over(partition by year order by pts desc) pts_rank
	from {{ ref('stg_season_stats') }}
)
select * from top_pts where pts_rank <= 3
