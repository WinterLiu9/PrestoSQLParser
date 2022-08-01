with exchange_rates as (
    select
        grass_date as exchange_date
         , exchange_rate as usd_2_cop
    from mp_order.dim_exchange_rate__reg_s0_live
    where 1=1
      and grass_region = 'CO'
)

   , users as (
    select
        u.user_id
         , case
               when status = 1 then 'normal'
               when status = 2 then 'banned'
               when status = 3 then 'banned'
               when status = 0 then 'delete'
               else 'non status'
        end as user_status
         , from_unixtime(u.registration_timestamp) at time zone 'America/Bogota' as user_created_at
    from mp_user.dim_user__co_s0_live u --- change to reg table
    where 1=1
      and u.grass_date = (select max(grass_date) as t from mp_user.dim_user__co_s0_live c)
      and status != 0
)

   , chot_base_information as (
    select
        userid as buyer_id
         , user_status
         , checkoutid as checkout_id
         , from_unixtime(chot.ctime) at time zone 'America/Bogota' as chot_created_at
    , (total_price/100/1000) / e.usd_2_cop as gmv_usd
    , ord.buyer_shipping_address_state as state
    , ord.buyer_shipping_address_city as city
    , split(replace(ord.payment_channel,'['),' ') as split_payment_channel
    , case when cardinality(split(replace(ord.payment_channel,'['),' ')) >= 1 then split(replace(ord.payment_channel,'['),' ') [1] else null end as pos1
    , case when cardinality(split(replace(ord.payment_channel,'['),' ')) >= 2 then split(replace(ord.payment_channel,'['),' ') [2] else null end as pos2
    , case when cardinality(split(replace(ord.payment_channel,'['),' ')) >= 3 then split(replace(ord.payment_channel,'['),' ') [3] else null end as pos3
    from marketplace.shopee_checkout_v2_db__checkout_v2_tab__co_daily_s0_live chot
        left join exchange_rates e
    on cast(from_unixtime(chot.ctime) at time zone 'America/Bogota' as date) = e.exchange_date
        left join mp_order.dwd_order_item_all_ent_df__co_s0_live ord
        on chot.checkoutid = ord.checkout_id
        left join users us
        on ord.buyer_id = us.user_id
    where 1=1
      and chot.payment_type in (1,87,18)
      and date(from_unixtime(chot.ctime) at time zone 'America/Bogota') >= current_date - interval '7' day
    -- and chot.userid in (595339883,617816661)
    -- and chot.userid in (513988215)
    group by 1,2,3,4,5,6,7,8,9,10
    order by 1,3
)


-- /*
   , rule_tester as (
    select
        *
         , sum(case when gmv_usd >= 25 then 1 else 0 end) over (partition by buyer_id) >= 5 as rule1
    from chot_base_information
    order by 1,3
)


-- /*
   , ordered_broked_ruled as (
    select
        buyer_id
         , user_status
         , cast(checkout_id as varchar) as checkout_id
         , chot_created_at
         , state
         , city
         , case
               when pos1 in ('SUDAMERIS') then 'BANCO GNB'
               when pos1 in ('BANCOOMEVA') then 'COOMEVA'
               when pos2 in ('DAVIVIENDA','COOMEVA','POPULAR') then pos2
               when pos2 = 'BILBAO' then 'BBVA'
               when pos1 in ('A') then concat(pos1,' ',pos2,' ',pos3)
               when pos1 in ('BANCO','A') and pos2 = 'DE' then concat(pos1,' ',pos2,' ',pos3)
               when pos1 in ('BANCO','AV','CAJA') then concat(pos1,' ',pos2)
               else pos1
        end as banco
    from rule_tester
    where 1=1
      and rule1
    order by 1,3
)


-- /*
   , structerd_cases as (
    select
        buyer_id
         , user_status
         , min(date(chot_created_at)) as first_date_involved
         , max(date(chot_created_at)) as last_date_involved
         , array_agg(distinct banco)[1] as banco
         , array_agg(distinct state)[1] as state
         , case
               when array_agg(distinct city)[1] in ('Atlántico','Bogotá, D.C.','Bolívar','Caldas','Magdalena','Valle Del Cauca') then array_agg(distinct city)[1]
               else array_agg(distinct state)[1]
        end as city
         , array_agg(distinct checkout_id) as last_chots_involved
    from ordered_broked_ruled
    where 1=1
    group by 1,2
)
/*
, whitelist as (
select *
from cobi_opsfraud.co_manual_whitelist
where 1=1
     and entity_type in ('buyer_id','seller_id')
)

, fptesting_users as (
select
    entity_id as user_id
from cobi_opsfraud.co_rule_dictamination_prod
where 1=1
    and lower(new_user_status) = 'fptesting'
    and entity_type = 'buyer_id'
)
*/
-- /*
select
    current_date as insert_data
        , 'buyer_id' as entity_type
        , cast(buyer_id as varchar) as entity_id
        , user_status as entity_status
        , last_date_involved date_rule
        , last_chots_involved as cases_involved
        , 'checkouts' as type_of_cases
        , case
    when state = 'Caldas' and  city = 'Villamaría' then 'direct ban'
    when state = 'Tolima' and  city = 'Tolima' then 'direct ban'
    when state = 'Atlántico' and  city = 'Barranquilla' and banco = 'AMEX' then 'direct ban'
    when state = 'Bogotá, D.C.' and  city = 'Bogotá, D.C.' and banco = 'AV VILLAS' then 'direct ban'
    when state = 'Bogotá, D.C.' and  city = 'Bogotá, D.C.' and banco = 'REDEBAN' then 'direct ban'
    when state = 'Bogotá, D.C.' and  city = 'Bogotá, D.C.' and banco = 'SERVIBANCA' then 'direct ban'
    when state = 'Bolívar' and  city = 'Cartagena De Indias' and banco = 'BANCO DE OCCIDENTE' then 'direct ban'
    when state = 'Bolívar' and  city = 'Cartagena De Indias' and banco = 'BBVA' then 'direct ban'
    when state = 'Bolívar' and  city = 'Cartagena De Indias' and banco = 'RAPPIPAY' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'AMEX' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'BANCO DE BOGOTA' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'BANCO DE OCCIDENTE' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'BBVA' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'CAJA SOCIAL' then 'direct ban'
    when state = 'Caldas' and  city = 'Manizales' and banco = 'COLPATRIA' then 'direct ban'
    when state = 'Cesar' and  city = 'Cesar' and banco = 'BANCO DE BOGOTA' then 'direct ban'
    when state = 'Cesar' and  city = 'Cesar' and banco = 'BANCO DE OCCIDENTE' then 'direct ban'
    when state = 'Cesar' and  city = 'Cesar' and banco = 'BANCOLOMBIA' then 'direct ban'
    when state = 'Magdalena' and  city = 'Santa Marta' and banco = 'BANCO DE OCCIDENTE' then 'direct ban'
    when state = 'Magdalena' and  city = 'Santa Marta' and banco = 'BBVA' then 'direct ban'
    when state = 'Magdalena' and  city = 'Santa Marta' and banco = 'CAJA SOCIAL' then 'direct ban'
    when state = 'Magdalena' and  city = 'Santa Marta' and banco = 'FINANDINA' then 'direct ban'
    when state = 'Sucre' and  city = 'Sucre' and banco = 'REDEBAN' then 'direct ban'
    when state = 'Valle Del Cauca' and  city = 'Cali' and banco = 'BANCO CREDIFINANCIERA' then 'direct ban'
    when state = 'Valle Del Cauca' and  city = 'Cali' and banco = 'DAVIVIENDA' then 'direct ban'
    when state = 'Valle Del Cauca' and  city = 'Cali' and banco = 'SERFINANZA' then 'direct ban'
    when state = 'Valle Del Cauca' and  city = 'Zarzal' and banco = 'BANCOLOMBIA' then 'direct ban'
    when state = 'Bolívar' and  city = 'Cartagena De Indias' and banco = 'COLPATRIA' then 'direct ban'
    when state = 'Tolima' and  city = 'Tolima' and banco = 'BANCO CREDIFINANCIERA' then 'direct ban'
    when state = 'Bolívar' and  city = 'Cartagena De Indias' and banco = 'AMEX' then 'direct ban'
    else 'check required'
    end as type_investigation
        , 'r0001' as broked_rule
        , (select agent from cobi_opsfraud.rule_asignment_prod where index = 'r0001') as agent
from structerd_cases u
/*
left join whitelist w
    on cast(u.buyer_id as varchar) = cast(w.entity_id as varchar)
left join fptesting_users fp
    on cast(u.buyer_id as varchar) = cast(fp.user_id as varchar)
*/
where 1=1
  and user_status != 'banned'
--    and coalesce(cast(w.entity_id as varchar),cast(fp.user_id as varchar)) is null
order by 3 desc
-- */