--******************************************************************************************************
--  Process Name : MEP - Offer Assignment
--  Script Name  : offer_file_preparation.hql
--  Description  : This script will prepare the fia offer file data.
--  Parameters   : fia_campaign_no
--  Input Files  : 
--  Input Table  : FiaTbl
--  Output File  : FiaOfferFile
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  sgopa4            1.0              12/29/2014                 SPE Changes
--  phala1            2.0              01/13/2015                 Backfill SPE Changes                                                                                   
-----------------------------------------------------------------------------------------

INSERT OVERWRITE DIRECTORY '${FiaOfferFile}' 
SELECT CONCAT(
COALESCE(campaign_nbr,' '),'|',
COALESCE(dept_nbr,' '),'|',
COALESCE(subclass_nbr,' '),'|',
COALESCE(val_offer_type_code,' '),'|',
COALESCE(val_offer_type_desc,' '),'|',
COALESCE(mds_fam_id,' '),'|',
COALESCE(investment_cnt,' '),'|',
COALESCE(value_amt,' '),'|',
COALESCE(value_pct,' '),'|',
COALESCE(min_item_purch_qty,' '),'|',
COALESCE(max_redemption_cnt,' '),'|',
COALESCE(package_code,' '),'|',
COALESCE(package_desc,' '),'|',
COALESCE(vendor_funded_ind,' '),'|',
COALESCE(coupon_item_nbr,' '),'|',
COALESCE(val_item_type_code,' '),'|',
COALESCE(val_item_type_desc,' '),'|',
COALESCE(value_coupon_nbr,' '),'|',
COALESCE(prvdr_coupon_nbr,' '),'|',
COALESCE(club_avail_ind,' '),'|',
COALESCE(filler_data,' ')
) FROM ${FiaTbl}
WHERE campaign_nbr=${fia_campaign_no};