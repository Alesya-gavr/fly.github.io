with user_address as
(select a.user_id, 
 case
 when a.country = 'Russia' then 'yes'
 when a.country is null then null
 else 'no'
 end as is_russia
from gd2.addresses a),
user_phone as
(select u.id as user_id, 
 case
  when left(u.phone, 1)='7' and length(u.phone) = 11 then 'yes'
  when u.phone is null then null
  else 'no'
  end as is_russia
  from gd2.users u),
 user_ip_decimal as
(select
            u.id as user_id
            , (u.last_sign_in_ip::inet-'0.0.0.0'::inet)as last_ip
        from gd2.users u), 
russian_or_undefined_ip as
(select *
 from gd2.ip2location i
 where i.country_name in ('Russian Federation', '-')),
 russian_or_undefined_user_ip as
(select uid.user_id, ru.country_name
 from user_ip_decimal uid
 join russian_or_undefined_ip ru on uid.last_ip >= ru.ip_from and uid.last_ip <= ru.ip_to),
user_ip as
(select u.id as user_id, 
 case
 when ru.country_name = 'Russian Federation' then 'yes'
 when ru.country_name = '-' then null
 when u.last_sign_in_ip is null then null --у нас есть пустые last_sign_in_ip в таблице users, чтобы не выводило NO на них, нужно это условие тоже
 else 'no'
 end as is_russia
 from gd2.users u
 left join russian_or_undefined_user_ip ru on u.id = ru.user_id),
 users_with_geo as
 (select  uid.user_id user_id, 
 coalesce(ua.is_russia, up.is_russia, ui.is_russia) is_russia
  from user_ip_decimal uid
  left join user_address ua on uid.user_id=ua.user_id 
  left join user_phone up on uid.user_id=up.user_id 
  left join user_ip ui on uid.user_id=ui.user_id
  where coalesce (ua.is_russia, up.is_russia, ui.is_russia) is not null),
  users_with_geo_purchases as
  (select uwg.user_id user_id, uwg.is_russia is_russia, date_trunc('month',p.created_at) months, p.state state, p.id orders
  from users_with_geo uwg
  left join gd2.purchases p
  on uwg.user_id=p.user_id),
  b as
   (select uwgp.user_id users, uwgp.months monthes,uwgp.is_russia is_russia, uwgp.orders, uwgp.state as state
   from users_with_geo_purchases uwgp
   where uwgp.state='successful'),
  first_purchases as
(select distinct p.user_id users, p.created_at created_at, min(date_trunc('month', p.created_at)) min_date
from gd2.purchases p
where p.state='successful'
group by 1,2
order by 3),
c as
(select b.users users, count(b.orders) orders,b.is_russia is_russia, fp.min_date min_date, b.monthes monthes, fp.created_at created_at
from b left join first_purchases fp
on b.users=fp.users
group by 1,3,4,5,6
order by 4,5),
d as
(select p.user_id user_id, lead(p.created_at) over(partition by p.user_id)-p.created_at diff
 from gd2.purchases p),
 e as
 (select c.users, avg(d.diff)
 from c join d
 on c.users=d.user_id
 where c.users='470994'
 group by 1),
 f as
(select count(distinct c.users)
 from c
 where c.monthes='2020-06-01' and c.orders>=1),
 g as
 (select p.user_id user_id, avg(p.amount) amount, p.state state
 from gd2.purchases p
 where p.state='successful' 
 group by 1,3),
 h as 
 (select percentile_cont(0.8)within group(order by g.amount) avg80, g.user_id  user_id
 from g
 group by 2)
 select count(c.users), h.avg80 avg80
 from c right join h on c.users=h.user_id
 where c.created_at='2020-06-30' and h.avg80>750
 group by 2
 
 
 