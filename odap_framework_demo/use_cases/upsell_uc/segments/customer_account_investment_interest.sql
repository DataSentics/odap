-- Databricks notebook source
SELECT
  c.customer_id,
  a.account_id
FROM
  ${env:READ_ENV}_odap_features_customer.features_latest AS c
INNER JOIN
  ${env:READ_ENV}_odap_features_account.features_latest AS a
ON
  c.customer_id == a.customer_id
WHERE
  c.investice_web_visits_count_in_last_30d > 0 AND
  a.incoming_transactions_sum_amount_in_last_90d >= 150000
