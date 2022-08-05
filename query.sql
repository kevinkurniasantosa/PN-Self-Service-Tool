-- ################################################################## CASE 4 

-- Certain category buyers who haven't shopped other main category
with a as (
select distinct
    u.userid
    from user_profile as u
    join order_mart__order_item_profile as p
    on u.userid = p.userid
    where
    p.main_category in (%s)
    and
    last_login >= current_date - interval '%s' day
    and
    u.status = 1
    and
    is_buyer = 1
),

--shopped certain category
b as
(
select distinct
userid
from order_mart__order_item_profile
where 
main_category in (%s)
)

--final
select distinct
concat('user_id=',cast(a.userid as varchar)) list_userid
from a
left join b
on a.userid = b.userid
where b.userid is null

-- ################################################################## CASE 5

-- Certain category buyers who haven't shopped other sub-category
with a as (
select distinct
    u.userid
    from user_profile as u
    join order_mart__order_item_profile as p
    on u.userid = p.userid          
    where
    p.main_category in (%s)
    and
    last_login >= current_date - interval '%s' day
    and
    u.status = 1
    and
    is_buyer = 1
),

--shopped certain category
b as
(
select distinct
userid
from order_mart__order_item_profile
where 
sub_category in (%s)
)

--final
select distinct
concat('user_id=',cast(a.userid as varchar)) list_userid
from a
left join b
on a.userid = b.userid
where b.userid is null

-- ################################################################## CASE 6 

-- Male/female buyers that login within the last certain days
select distinct
    concat('user_id=',cast(u.userid as varchar)) list_userid
    from user_profile as u
    join order_mart__order_item_profile as p
    on u.userid = p.userid 
    where
    last_login >= current_date - interval '30' day
    and
    u.status = 1
    and
    u.gender in (1,3)

-- ################################################################## CASE 6

-- Certain category & sub-category buyers who login within the last certain days  
select distinct
    concat('user_id=',cast(u.userid as varchar)) list_userid
    from user_profile as u
    join order_mart__order_item_profile as p
    on u.userid = p.userid
    where
    (p.main_category in (%s)
    or
    p.sub_category in (%s))
    and
    last_login >= current_date - interval '%s' day
    and
    u.status = 1
    and
    u.is_buyer = 1

-- ################################################################## CASE 7

-- Certain category and sub-category buyers that login within the last certain days who haven't shopped during certain period
with a as(
select distinct
    u.userid
    from user_profile as u
    join order_mart__order_item_profile as p
    on u.userid = p.userid
    where
    (p.main_category in (%s)
    or
    p.sub_category in (%s))
    and
    last_login >= current_date - interval '%s' day
    and
    u.status = 1
    and
    u.is_buyer = 1
),

--shopped for the last 30 days
b as
(
select distinct
userid
from order_mart__order_item_profile
where grass_date between date'%s' and date'%s'
)

--final
select distinct
concat('user_id=',cast(a.userid as varchar)) list_userid
from a
left join b
on a.userid = b.userid
where b.userid is null

-- ################################################################### CASE 8

-- PN Shop followers
select
'user_id='||cast(userid as varchar) as userid
from shopee_follower_id_db__shop_follow_tab
where shopid = %s
and status = 1

-- ################################################################### CASE 9

-- Digital product buyers who login within the last certain days
with a as(
select distinct
    u.userid
    from user_profile as u
    where
    last_login >= current_date - interval '%s' day
    and
    u.status = 1
    and 
    u.is_buyer=1
),

--DP Buyers
b as(
select distinct o.user_id as userid
from shopee_digital_product_order_id_db__order_tab as o
)

select distinct 
concat('user_id=',cast(a.userid as varchar))
from a
join b 
on a.userid = b.userid

-- ################################################################### CASE 10

-- Users that login within the last certain days who haven't shopped digital products
with b as(
        select distinct o.user_id as userid
        from shopee_digital_product_order_id_db__order_tab AS o
    )

select distinct 
    concat('user_id=',cast(u.userid as varchar)) list_userid
    from user_profile as u 
    where
    last_login >= current_date - interval '%s' day
    and 
    u.userid not in (select distinct userid from b)
    and 
    u.status = 1

-- ################################################################### CASE 11

-- Certain category buyers that login within the last certain days who haven't shopped digital products
with b as(
select distinct o.user_id as userid
from shopee_digital_product_order_id_db__order_tab AS o
)

select distinct 
    concat('user_id=',cast(u.userid as varchar)) list_userid
    from user_profile as u 
    join order_mart__order_item_profile as p 
    on u.userid = p.userid
    where
    p.main_category in (%s) 
    and
    last_login >= current_date - interval '%s' day
    and 
    u.userid not in (select distinct userid from b)
    and 
    u.status = 1

-- ###################################################################  CASE 12

-- Users filtered by Item/Product ID who shopped from certain date until certain date
select distinct
concat('user_id=',cast(p.userid as varchar)) as list_userid
from order_mart__order_item_profile as p
where itemid in (%s)
and grass_date between date'%s' and date'%s'

-- ###################################################################  CASE 13

-- Goyang Shopee User
select distinct
concat('user_id=',cast(a1.userid as varchar)) list_userid
from shopee_bi_id_active_user_table a1
where a1.grass_date <= current_date - interval '1' day
and a1.grass_date >= current_date - interval '8' day
and a1.userid in (select distinct userid from shopee_coins_id_db__coin_transaction_tab where lower(from_utf8(from_base64(info))) like '%goyang shopee%')

-- Goyang Shopee User modified
select distinct
concat('user_id=',cast(a1.userid as varchar)) as list_userid
from user_login_record_id_db__user_login_record_tab as a1
where a1.grass_date between date'2019-07-26' and date'2019-07-28'
and a1.userid in (select distinct userid from shopee_coins_id_db__coin_transaction_tab where lower(from_utf8(from_base64(info))) like '%goyang shopee%')

-- ###################################################################  CASE 14

-- Quiz User
select 
    a.event_id,
    c.name as event_name,
    a.session_id,
    b.name as session_name,
    date(from_unixtime(b.start,'Asia/Jakarta')) as session_date,
    a.userid, 
    username
    case when date(u.registration_time  - interval '1' hour) = date(from_unixtime(b.start,'Asia/Jakarta')) then 'NEW' end as new_indicator,
    case 
        when gender = 1 then 'MALE'
        when gender = 2 then 'FEMALE'
        when gender = 3 then 'PREDICTED MALE' 
        when gender = 4 then 'PREDICTED FEMALE' 
        else 'UNKNOWN'
    end as gender
    from shopee_id_bi_team__kuis_players a
    join shopee_gamehq_id_db__hq_sessions_tab b on a.session_id = cast(b.id as VARCHAR)
    join shopee_gamehq_id_db__hq_events_tab c on a.event_id = cast(c.id as VARCHAR)
    join user_profile u on a.userid = cast(u.userid as VARCHAR)
where date(from_unixtime(b.start,'Asia/Jakarta')) >= now() - interval '30' day;

-- ###############################################

-- Quiz User modified
select distinct
concat('user_id=',cast(a.userid as varchar)) as list_userid
from shopee_id_bi_team__kuis_players as a
join shopee_gamehq_id_db__hq_sessions_tab as b 
on a.session_id = cast(b.id as varchar)
join shopee_gamehq_id_db__hq_events_tab as c 
on a.event_id = cast(c.id as varchar)
where date(from_unixtime(b.start,'Asia/Jakarta')) >= date('2019-06-26')
and date(from_unixtime(b.start,'Asia/Jakarta')) <= date('2019-06-28')

-- ###################################################################  CASE 15

-- 1 dollar game User
SELECT grass_date, userid, orderid
from order_mart__order_item_profile
where bi_excluded ='OneDollarGame'
and grass_date >= current_date - interval '30' day

-- 1 dollar game User modified
select distinct
concat('user_id=',cast(p.userid as varchar)) as list_userid
from order_mart__order_item_profile as p
where bi_excluded = 'OneDollarGame'
and grass_date between date'2019-07-26' and date'2019-07-28'

-- ###################################################################  CASE 16

-- Daily Prize
SELECT
    distinct date(date_parse(cast(dp.date as varchar),'%Y-%m-%d %H:%i:%s')) datte,
    cast(dp.user_id as INT) userid,
    CASE
        WHEN date(date_add('hour',-1,u.registration_time)) = date(date_parse(cast(dp.date as varchar),'%Y-%m-%d %H:%i:%s')) then 'new_user'
        ELSE 'ret_user'
    END AS user_type,
    CASE
        WHEN u.gender = 1 or u.gender = 3  then 'male'
        WHEN u.gender = 2 or u.gender = 4  then 'female'
        else 'unknown'
       END gender,
    'Daily Prize' as game
FROM
    shopee_id_mk_team__daily_prize dp
LEFT JOIN user_profile u on u.userid=cast(dp.user_id as INT)
WHERE
    date(date_parse(cast(dp.date as varchar),'%Y-%m-%d %H:%i:%s'))>=date('2019-06-01')
    AND date(date_parse(cast(dp.date as varchar),'%Y-%m-%d %H:%i:%s'))<=date('2019-06-20')

-- Daily Prize modified
select distinct
concat('user_id=',cast(dp.user_id as varchar)) as list_userid
from shopee_id_mk_team__daily_prize dp
left join user_profile as u 
on u.userid = cast(dp.user_id as int)
where date(date_parse(cast(dp.date as varchar), '%Y-%m-%d %H:%i:%s')) >= date('2019-06-01')
and date(date_parse(cast(dp.date as varchar), '%Y-%m-%d %H:%i:%s')) <= date('2019-06-20')

-- Daily Prize modified 2
select distinct
concat('user_id=',cast(dp.user_id as varchar)) as list_userid
from shopee_id_mk_team__daily_prize_vfive as dp
left join user_profile as u 
on u.userid = cast(dp.user_id as int)
where date(date_parse(cast(dp.datte as varchar), '%Y-%m-%d %H:%i:%s')) >= date('2019-06-26')
and date(date_parse(cast(dp.datte as varchar), '%Y-%m-%d %H:%i:%s')) <= date('2019-06-28')

-- ###################################################################  CASE 17

-- Male Quiz Shopee Users
select distinct
concat('user_id=',cast(a.userid as varchar)) as list_userid
from shopee_id_bi_team__kuis_players as a
left join (
    select distinct u.userid
    from user_profile as u 
on a.userid = cast(u.userid as varchar)
)
where u.gender in (1,3)
join shopee_gamehq_id_db__hq_sessions_tab as b 
on a.session_id = cast(b.id as varchar)
join shopee_gamehq_id_db__hq_events_tab as c 
on a.event_id = cast(c.id as varchar)
where date(from_unixtime(b.start,'Asia/Jakarta')) >= date('2019-06-26')
and date(from_unixtime(b.start,'Asia/Jakarta')) <= date('2019-06-28')



JOIN 
    (
    SELECT  distinct parent_key as checkoutid,
            channelid
    FROM shopee_id_db__checkout_tab_checkout_info_checkout_payment_info
    WHERE channelid = 80030
    ) as c
ON c.checkoutid = o.checkoutid


