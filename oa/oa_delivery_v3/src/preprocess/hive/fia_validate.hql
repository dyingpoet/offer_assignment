select 
b.category_nbr
, b.sub_category_nbr
, b.system_item_nbr
from pythia.offer_assignment_fia a
join sams_us_clubs.item_info b 
on a.system_item_nbr = b.system_item_nbr
where a.campaign_month='Apr2014-gec';


