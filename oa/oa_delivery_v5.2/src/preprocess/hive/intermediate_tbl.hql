CREATE EXTERNAL TABLE IF NOT EXISTS pythia.fia_subcat_item_club (   
                cat_subcat_nbr STRING
                , system_item_nbr INT
                , club_nbr INT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '/user/pythia/Workspaces/SamsMEP/fia_subcat_item_club'
;


CREATE EXTERNAL TABLE IF NOT EXISTS pythia.offer_assigned_member_base_w_club_preference (   
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , preferred_club_nbr INT
                , issuing_club_nbr INT
                , assigned_club_nbr INT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '/user/pythia/Workspaces/SamsMEP/offer_assigned_member_base_w_club_preference'
;



