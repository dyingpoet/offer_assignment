use pythia;
select cat_subcat_nbr, SUM(score) from pythia.pis_member_reco_score_anchor where campaign_month='Apr2014-gec' group by cat_subcat_nbr ;


