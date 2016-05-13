

SELECT visit_nbr,
  store_nbr,
  unit_qty,
  retail_price,
  card_holder_nbr,
  membership_nbr,
  scan_id AS system_item_nbr,
  category_nbr,
  sub_category_nbr,
  visit_date
(SELECT scan_data.visit_nbr AS visit_nbr,
        scan_data.store_nbr AS store_nbr,
        scan_id,
        unit_qty,
        retail_price,
        card_holder_nbr,
        membership_nbr,
        visit_date
FROM
(SELECT visit_nbr,
        store_nbr,
        scan_id,
        unit_qty,
        retail_price,
        visit_date
      FROM us_wc_mb_tables.scan
      WHERE visit_date >= '${hiveconf:StartDate}' and visit_date <= '${hiveconf:EndDate}') scan_data JOIN
(SELECT store_nbr,
        visit_nbr,
        card_holder_nbr,
        membership_nbr,
        visit_date
      FROM us_wc_mb_tables.visit_member
      WHERE visit_date >= '${hiveconf:StartDate}' and visit_date <= '${hiveconf:EndDate}'
      AND mbr_country_code = 'US') visit_mem_info
      ON (visit_mem_info.store_nbr = scan_data.store_nbr AND visit_mem_info.visit_nbr = scan_data.visit_nbr)) scan_visit_data JOIN
  (SELECT mds_fam_id,
      dept_nbr AS category_nbr,
      subclass_nbr AS sub_category_nbr
    FROM common_transaction_model.item_dim
    WHERE country_code = 'US' AND
      base_div_nbr = 18 AND
      current_ind = 'Y')item_info
  ON (scan_visit_data.scan_id = item_info.mds_fam_id)




