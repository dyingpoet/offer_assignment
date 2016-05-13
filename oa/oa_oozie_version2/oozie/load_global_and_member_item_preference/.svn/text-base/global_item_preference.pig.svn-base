-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : global_item_preference.pig
--  Description  : This pig script will create global item preference details.
--  Input Files  : TransData,FilterClub,ItemSubCatMapping
--  Output Files : OutputGlobalPref
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--                                                                                     
-----------------------------------------------------------------------------------------
set default_parallel 800

-----------------------------------------------------------------------------------------
-- Load Raw Transaction Data
-----------------------------------------------------------------------------------------

TransData00 = LOAD '$TransData' USING PigStorage('\u0001') AS
         (visit_nbr: int,
         club_nbr: int,
         unit_qty: double,
         retail_price: double,
         card_holder_nbr: int,
         membership_nbr: int,
         system_item_nbr: int,
         category_nbr: int,
         sub_category_nbr: int,
         visit_date: chararray);

-----------------------------------------------------------------------------------------
-- Time Decayed Transactions
-----------------------------------------------------------------------------------------

TransData00 = foreach TransData00 generate
         visit_nbr,
         club_nbr,
         unit_qty,
         retail_price,
         card_holder_nbr,
         membership_nbr,
         system_item_nbr,
         category_nbr,
         sub_category_nbr,
         visit_date
;

filterClub = LOAD '$FilterClub' USING PigStorage('\u0001') AS (club_nbr:int);

TransData56 = JOIN TransData00 by (int)club_nbr, filterClub by club_nbr using 'replicated';

TransData01 = FOREACH TransData56 GENERATE
           TransData00::visit_nbr AS          visit_nbr
         , TransData00::club_nbr AS          club_nbr
         , TransData00::unit_qty AS          unit_qty
         , TransData00::retail_price AS          retail_price
         , TransData00::card_holder_nbr AS          card_holder_nbr
         , TransData00::membership_nbr AS          membership_nbr
         , TransData00::system_item_nbr AS          system_item_nbr
         , TransData00::category_nbr AS category_nbr
         , TransData00::sub_category_nbr AS sub_category_nbr
         , TransData00::visit_date AS          visit_date
;

ItemSubcat = LOAD '$ItemSubCatMapping' USING PigStorage('\u0001') AS 
	(system_item_nbr: int,
  	category_nbr: int,
  	sub_category_nbr: int);

TransData02 = JOIN TransData01 BY system_item_nbr, ItemSubcat BY system_item_nbr ;

TransData0 = FOREACH TransData02 GENERATE
		   TransData01::visit_nbr AS          visit_nbr
         , TransData01::club_nbr AS          club_nbr
         , TransData01::unit_qty AS          unit_qty
         , TransData01::retail_price AS          retail_price
         , TransData01::card_holder_nbr AS          card_holder_nbr
         , TransData01::membership_nbr AS          membership_nbr
         , TransData01::system_item_nbr AS          system_item_nbr
         , ItemSubcat::category_nbr AS category_nbr
         , ItemSubcat::sub_category_nbr AS sub_category_nbr
         , TransData01::visit_date AS          visit_date;
         
TransData2 = FOREACH TransData0 GENERATE membership_nbr AS membership_nbr,
                                         card_holder_nbr AS card_holder_nbr,
                                         category_nbr AS category_nbr,
                                         sub_category_nbr AS sub_category_nbr,
                                         system_item_nbr AS system_item_nbr,
                                         unit_qty AS unit_qty,
                                         visit_date AS visit_date;

-----------------------------------------------------------------------------------------
-- keep only transactions 1) with positive quantities, 2) visit date within a certain range, 
-- 3) valid cat, sub cat nbr, 4) non-null membership_nbr, card_holder_nbr
-----------------------------------------------------------------------------------------

TransData3 = FILTER TransData2 BY (unit_qty > 0.0 AND
				   visit_date >= '$DataStartT' AND visit_date <= '$DataEndT' AND
                                   category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
                                   sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
                                   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransData4 = FOREACH TransData3 GENERATE membership_nbr,
                                         card_holder_nbr,
                                        CONCAT((SIZE((chararray)category_nbr) == 1 ? CONCAT('0', (chararray)category_nbr) : (chararray)category_nbr),
                                        (SIZE((chararray)sub_category_nbr) == 1 ? CONCAT('0', (chararray)sub_category_nbr) : (chararray)sub_category_nbr)) AS cat_subcat_nbr,
                                        system_item_nbr,
                                        unit_qty,
                                        visit_date;

TransData8 = FOREACH TransData4 GENERATE membership_nbr AS membership_nbr,
                                         card_holder_nbr AS cardholder_nbr,
                                         cat_subcat_nbr AS cat_subcat_nbr,
                                         system_item_nbr AS system_item_nbr,
                                         unit_qty AS unit_qty,
                                         visit_date AS visit_date;

-----------------------------------------------------------------------------------------
-- keep transactions that are during the period of [EligibleStartT, EligibleEndT] and truly belong to this member-cardholder
-- apply eligibility rules to get users qualified for reward
-- group data to get the study set
-----------------------------------------------------------------------------------------

TransData10 = GROUP TransData8 BY (cat_subcat_nbr, system_item_nbr);

TransData11 = FOREACH TransData10 GENERATE group.cat_subcat_nbr AS cat_subcat_nbr, group.system_item_nbr AS system_item_nbr, COUNT(TransData8) AS countVisits;

STORE TransData11 INTO '$OutputGlobalPref' Using PigStorage('\u0001');