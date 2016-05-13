
CampaignNbr=308
CAMPAIGN_TYPE=RECOMMEND
MEMBER_LB=1
MEMBER_UB=4
WORK_DIR=/home/jli21/sams/working_session
PIG=/usr/bin/pig
PYTHON=/home/jli21/lib/canopy/appdata/canopy-1.3.0.1715.rh5-x86_64/bin/python2.7
OfferFile=/home/jli21/sams/oa_module/postprocess/offer_gec_rec_03172014.txt
DS_RUN=2014-04-14
DATA_START_T=2013-04-15
DATA_END_T=2014-04-14
DS_OBSOLETE_DATE=2014-03-14
DS_ITEM_MAPPING=2014-03-20
DS_INVENTORY=2014-03-20
MEMBERSHIP_DIM_PARTITION_DATE=2014-03-10
CAMPAIGN_MONTH=Apr2014
CAMPAIGN_MONTH_TYPE=Apr2014-gec
SCORE_SOURCE=GeC
SCORE_TYPE=rec
NUM_MEMBERS=342713
DELTA=0.02
DATABASE=pythia



#step0
ITEM_INFO_SOURCE_TBL=sams_us_clubs.item_info_history
ITEM_INFO_TBL=pythia.item_info_snapshot
ITEM_INFO_LOC=/user/pythia/Workspaces/SamsMEP/item_info_snapshot

#step1
TRANS_TBL=pythia.customer_club_day_item_sales
TRANS_LOC=/user/pythia/Workspaces/SamsMEP/customer_club_day_item_sales_with_new_sub_category2013_2014
TRANS_LOC_PARTITION=$TRANS_LOC/ds=$DS_RUN
VISIT_MEMBER_TBL=us_wc_mb_tables.visit_member
SCAN_TBL=us_wc_mb_tables.scan
ITEM_DIM_TBL=common_transaction_model.item_dim


#step2
FILTER_CLUB=/user/jli21/sams/offer/56clubs.txt
MEM_COMBINED=/user/pythia/Workspaces/SamsMEP/TTEScoring/plus/init/2014-03-10/mem_with_compcards
ITEM_SUBCAT_MAPPING=/user/pythia/Workspaces/SamsMEP/item_info_snapshot/ds=2014-03-31
PREF_LOC=/user/pythia/Workspaces/SamsMEP/subcatItemPref

#step3
SAMS_MEMBERSHIP_DIM_TBL=customers.sams_us_clubs_sams_membership_dim
SAMS_CARDHOLDER_DIM_TBL=customers.sams_us_clubs_sams_mbr_cardholder_dim
INVENTORY_HIST_TBL=sams_us_clubs.club_item_inventory_history

FIA_TBL=pythia.offer_assignment_fia
OFFER_ASSIGNED_MEMBER_BASE_TBL=pythia.offer_assigned_member_base
RECO_LOC=/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/plus/init/20140317Decay/recommend_score_cobought_no_smoothing_max_raw_cobought_transaction_debug_12mon

TASK_TBL=pis_member_subcat_item_coupon_score
TASK_LOC=/user/pythia/Workspaces/SamsMEP/pis_member_subcat_item_coupon_score
RECO_TBL=pis_member_reco_score_anchor
MEMBER_CLUB_PREF_TBL=offer_assigned_member_base_w_club_preference
MEMBER_CLUB_PREF_LOC=/user/pythia/Workspaces/SamsMEP/offer_assigned_member_base_w_club_preference
SUBCAT_ITEM_CLUB_TBL=fia_subcat_item_club
SUBCAT_ITEM_CLUB_LOC=/user/pythia/Workspaces/SamsMEP/fia_subcat_item_club

#step4-6
LAST_OFFER_ASSIGNMENT_LOC=/user/pythia/Workspaces/SamsMEP/offer_assignment_output/campaign_month=2014feb
MEMBER_ITEM_PREF_LOC=/user/pythia/Workspaces/SamsMEP/subcatItemPref/member/ds=$DS_RUN
GLOBAL_ITEM_PREF_LOC=/user/pythia/Workspaces/SamsMEP/subcatItemPref/global/ds=$DS_RUN
SCORE_FLT_LOC=/user/pythia/Workspaces/SamsMEP/pis_member_subcat_item_coupon_score/campaign_month=$CAMPAIGN_MONTH_TYPE
SCORE_FLT_FINAL_LOC=/user/pythia/Workspaces/SamsMEP/pis_member_subcat_item_coupon_score_final/campaign_month=$CAMPAIGN_MONTH_TYPE
SCORE_FILE_LOC=/user/pythia/Workspaces/SamsMEP/pis_offer_assignment_score_file/campaign_month=$CAMPAIGN_MONTH_TYPE
MEMBER_FILE_LOC=/user/pythia/Workspaces/SamsMEP/pis_offer_assignment_member_file/campaign_month=$CAMPAIGN_MONTH_TYPE


#postprocess
SAMS_MEMBER_XREF_TBL=customers.sams_us_clubs_member_xref
SAMS_VALUE_COUPON_DIM_TBL=sams_us_clubs.value_coupon_dim
SAMS_VALUE_COUPON_ITEM_DIM_TBL=sams_us_clubs.value_coupon_item_dim

OA_OUTPUT_TBL=offer_assignment_output
OA_OUTPUT_LOC=/user/pythia/Workspaces/SamsMEP/offer_assignment_output

