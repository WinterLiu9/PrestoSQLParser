with user as (
    select user_id, shop_id, user_status, region
    from user.dim_user
    where grass_date = CURRENT_DATET - INTERVAL '1' day
    and grass_region = 'SG'
), login as (
    select user_id, count(*) login_c
    from user.dwd_login_ent_di
    where grass_date >= DATE('2023-01-01')
    and grass_region = 'SG'
    group by 1
)
select user.user_id, user_status, shop_id, login_c
from user left join login on user.user_id = login.user_id
;