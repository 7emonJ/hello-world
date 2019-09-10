SELECT acct_no,
       coll_org,
       in_days,
       loan_key,
       rest_principal,
       IF(in_od_days + in_days = now_od_days
          OR in_od_days + in_days = out_od_days, 1, 0) repay_label,
       IF(in_od_days + in_days = now_od_days
          OR in_od_days + in_days = out_od_days, rest_principal, 0) repay_rp
FROM
  (SELECT In_coll_org.acct_no,
          In_coll_org.coll_org,
          M_days.in_days,
          In_risk.loan_key,
          In_risk.rest_principal,
          In_risk.in_od_days,
          Now_risk.now_od_days,
          Out_risk.out_od_days,
          Pay_plan.pay_od_days
   FROM
     (SELECT acct_no,
             coll_org,
             over_due_days,
             remain_principal
      FROM
        (SELECT acct_no,
                coll_org,
                over_due_days,
                remain_principal,
                row_number() over(PARTITION BY acct_no
                                  ORDER BY dt) AS rank1
         FROM fdm.uni04_ermas_case_main_all
         WHERE dt BETWEEN date_format(date_sub(current_date(),1),'yyyy-MM-01') AND date_format(date_sub(current_date(),1),'yyyy-MM-dd')
           AND is_close = 'N'
           AND division_monthly_over_due_level = 'C-M2'
           AND coll_org IN ('3WWCM2A',
                            '3WWCM2B',
                            '3WWCM2C',
                            '3WWCM2D',
                            '3WWCM2E',
                            '3WWCM2F',
                            '3WWCM2G',
                            '3NCCM2')
           AND product_type = 'RRD') In_coll_org_raw
      WHERE rank1 = 1) In_coll_org
   LEFT JOIN
     (SELECT acct_no,
             coll_org,
             max(dt) AS max_dt,
             min(dt) AS min_dt,
			 datediff(max(dt), min(dt)) + 1 AS in_days,
             max(over_due_amt) AS max_amt
      FROM fdm.uni04_ermas_case_main_all
      WHERE dt BETWEEN date_format(date_sub(current_date(),1),'yyyy-MM-01') AND date_format(date_sub(current_date(),1),'yyyy-MM-dd')
        AND is_close = 'N'
        AND product_type = 'RRD'
        AND division_monthly_over_due_level = 'C-M2'
        AND coll_org IN ('3WWCM2A',
                         '3WWCM2B',
                         '3WWCM2C',
                         '3WWCM2D',
                         '3WWCM2E',
                         '3WWCM2F',
                         '3WWCM2G',
                         '3NCCM2')
      GROUP BY acct_no,
               coll_org)M_days ON M_days.acct_no = In_coll_org.acct_no
   AND M_days.coll_org = In_coll_org.coll_org
   LEFT JOIN
     (SELECT account_bill_key,
             proxy_serial_no
      FROM fdm.uni02_user_bill_chain
      WHERE dp = 'active' )b1 ON In_coll_org.acct_no = b1.proxy_serial_no
   LEFT JOIN
     (SELECT loan_key,
             bill_key
      FROM fdm.uni01_loan_chain
      WHERE dp = 'active'
        AND channel_code = 'RRD')b2 ON b2.bill_key = b1.account_bill_key
   LEFT JOIN
     (SELECT loan_key,
             od_days AS in_od_days,
             rest_principal,
             cal_time
      FROM gdm.agt_risk_loan_all
      WHERE cal_time BETWEEN date_format(date_sub(current_date(),1),'yyyy-MM-01') AND date_format(date_sub(current_date(),1),'yyyy-MM-dd')
        AND od_days>0
        AND chnl = 'RRD') In_risk ON In_risk.loan_key = b2.loan_key
   AND In_risk.cal_time = M_days.min_dt
   LEFT JOIN
     (SELECT loan_key,
             od_days AS now_od_days
      FROM gdm.agt_risk_loan_all
      WHERE dt = date_format(date_sub(current_date(),1),'yyyy-MM-dd')
        AND od_days >= 0
        AND chnl = 'RRD') Now_risk ON Now_risk.loan_key = In_risk.loan_key
   LEFT JOIN
     (SELECT loan_key,
             min(payoff_time),
             max(datediff(to_date(payoff_time),to_date(due_date))) AS pay_od_days
      FROM fdm.uni01_repay_plan_chain
      WHERE dp = 'active'
        AND business_chnl_code = 'RRD'
        AND to_date(due_date) < to_date(current_date())
        AND datediff(to_date(payoff_time), to_date(due_date)) > 0
        AND to_date(payoff_time) BETWEEN date_format(date_sub(current_date(),1),'yyyy-MM-01') AND date_format(date_sub(current_date(),1),'yyyy-MM-dd')
      GROUP BY loan_key
      ORDER BY loan_key) Pay_plan ON Pay_plan.loan_key = In_risk.loan_key
   LEFT JOIN
     (SELECT loan_key,
             od_days AS out_od_days,
             rest_principal,
             dt
      FROM gdm.agt_risk_loan_all
      WHERE dt BETWEEN date_format(date_sub(current_date(),1),'yyyy-MM-01') AND date_format(date_sub(current_date(),1),'yyyy-MM-dd')
        AND od_days>=0
        AND chnl = 'RRD') Out_risk ON Out_risk.loan_key = In_risk.loan_key
   AND Out_risk.dt = M_days.max_dt) Raw_data