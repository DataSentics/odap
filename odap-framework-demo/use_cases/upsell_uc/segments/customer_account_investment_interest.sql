-- Databricks notebook source
SELECT
  c.customer_id,
  a.account_id
FROM
  odap_features_customer.features_customer_latest AS c
INNER JOIN
  odap_features_account.features_account_latest AS a
ON
  c.customer_id == a.customer_id
WHERE
  c.investice_web_visits_count_in_last_30d > 0 AND
  a.incoming_transactions_sum_amount_in_last_90d >= 150000
