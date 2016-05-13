hive -e "use pythia; select distinct system_item_nbr from fia_subcat_item_club where campaign_month='Jun2014-gec' order by system_item_nbr;" > item_avail_gec_2014jun_v2
#hive -e "use pythia; select distinct system_item_nbr from offer_assignment_fia where campaign_month='Jun2014-gec' order by system_item_nbr;" > item_offer_gec_2014jun

#hive -e "use pythia; select distinct system_item_nbr from fia_subcat_item_club where campaign_month='Jun2014-isd' order by system_item_nbr;" > item_avail_isd_2014jun_v2
#hive -e "use pythia; select distinct system_item_nbr from offer_assignment_fia where campaign_month='Jun2014-isd' order by system_item_nbr;" > item_offer_isd_2014jun

