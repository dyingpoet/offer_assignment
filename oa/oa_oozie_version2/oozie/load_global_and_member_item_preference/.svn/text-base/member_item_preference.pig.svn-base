-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : member_item_preference.pig
--  Description  : This pig script will create member item preference details.
--  Input Files  : MemCombined,TransData,ItemSubCatMapping
--  Output Files : OutputMemberPref
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  cchan1             2.0              11/17/2014          Attach pscore member pool to OA                                                                                                                                                                    
--	cchan1		       3.0              01/12/2015			Include member pool from SPE
-----------------------------------------------------------------------------------------
set default_parallel 800

-----------------------------------------------------------------------------------------
-- Load member pool file for processing
-----------------------------------------------------------------------------------------

Mem0 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
membership_nbr: int,
membership_create_date: chararray,
issuing_country_code: chararray,
cardholder_nbr: int,
pdcardholder_nbr: int
);

Mem2 = FOREACH Mem0 GENERATE membership_nbr,
        membership_create_date,
        pdcardholder_nbr,
        cardholder_nbr;

-----------------------------------------------------------------------------------------
-- Load Transaction Data
-----------------------------------------------------------------------------------------

TransData0 = LOAD '$TransData' USING PigStorage('\u0001') AS
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

TransData10 = FOREACH TransData0 GENERATE visit_nbr,
                                         club_nbr,
                                         system_item_nbr,
                                         membership_nbr,
                                         card_holder_nbr,
                                         category_nbr,
                                         sub_category_nbr,
                                         visit_date,
                                         unit_qty,
                                         ((double) 1.0 / EXP( ( DaysBetween(ToDate('2014-12-30','yyyy-MM-dd'),ToDate(visit_date,'yyyy-MM-dd')))/365.0)) as retail_all,
                                         retail_price;

-----------------------------------------------------------------------------------------
-- Load Item Subcat Mapping Data
-----------------------------------------------------------------------------------------

ItemSubcat = LOAD '$ItemSubCatMapping' USING PigStorage('\u0001') AS (system_item_nbr: int, category_nbr: int, sub_category_nbr: int);

TransData1 = JOIN TransData10 BY system_item_nbr, ItemSubcat BY system_item_nbr ;

TransData2 = FOREACH TransData1 GENERATE TransData10::visit_nbr AS visit_nbr,
                                         TransData10::club_nbr AS club_nbr,
                                         TransData10::system_item_nbr AS system_item_nbr,
                                         TransData10::membership_nbr AS membership_nbr,
                                         TransData10::card_holder_nbr AS card_holder_nbr,
                                         ItemSubcat::category_nbr AS category_nbr,
                                         ItemSubcat::sub_category_nbr AS sub_category_nbr,
                                         TransData10::unit_qty AS unit_qty,
                                         TransData10::visit_date AS visit_date,
                                         TransData10::retail_all;

TransData3 = FILTER TransData2 BY (unit_qty > 0.0 AND retail_all > 0.0 AND
                                   (visit_date >= '$DataStartT' AND visit_date <= '$DataEndT') AND
                                   category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
                                   sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
                                   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransData4 = FOREACH TransData3 GENERATE membership_nbr,
                                         card_holder_nbr,
                                        CONCAT((SIZE((chararray)category_nbr) == 1 ? CONCAT('0', (chararray)category_nbr) : (chararray)category_nbr),
                                        (SIZE((chararray)sub_category_nbr) == 1 ? CONCAT('0', (chararray)sub_category_nbr) : (chararray)sub_category_nbr)) AS cat_subcat_nbr,
                                        system_item_nbr,
                                        retail_all,
                                        unit_qty,
                                        visit_date;

TransData5 = GROUP TransData4 BY (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date) PARALLEL 500;

TransData6 = FOREACH TransData5 GENERATE FLATTEN(group) AS (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date),
                                                        SUM(TransData4.retail_all) AS retail_all,
                                                        SUM(TransData4.unit_qty) AS unit_qty;

TransData7 = JOIN Mem2 BY (membership_nbr, cardholder_nbr), TransData6 BY (membership_nbr, card_holder_nbr) PARALLEL 500;

-----------------------------------------------------------------------------------------
-- Join transaction and member data by membership_nbr and card_holder_nbr
-----------------------------------------------------------------------------------------

TransData8 = FOREACH TransData7 GENERATE Mem2::membership_nbr AS membership_nbr,
        Mem2::pdcardholder_nbr AS pdcardholder_nbr,
        Mem2::cardholder_nbr AS cardholder_nbr,
        Mem2::membership_create_date AS membership_create_date,
        TransData6::cat_subcat_nbr AS cat_subcat_nbr,
        TransData6::system_item_nbr AS system_item_nbr,
        TransData6::visit_date AS visit_date,
        TransData6::unit_qty AS unit_qty,
        TransData6::retail_all AS retail_all;

TransData9 = FILTER TransData8 BY (visit_date >= membership_create_date);

TransData10 = GROUP TransData9 BY (membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr) PARALLEL 500;

TransData11 = FOREACH TransData10 GENERATE FLATTEN(group) AS (membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr),
TransData9.(visit_date, unit_qty, retail_all) AS trans;

TransData12 = FOREACH TransData11 {
                GENERATE membership_nbr,
                         cardholder_nbr,
                         membership_create_date,
                         cat_subcat_nbr,
                         system_item_nbr,
                         (trans IS NULL ? 0 : COUNT(trans)) AS total_visit;
}

-----------------------------------------------------------------------------------------
-- Store Final Data
-----------------------------------------------------------------------------------------

STORE TransData12 INTO '$OutputMemberPref' USING PigStorage('\u0001');
