-- Databricks notebook source
SELECT
  customer_id
FROM
  ${env:READ_ENV}.odap_features_customer.features_latest
WHERE
  pujcky_web_visits_count_in_last_30d > 0
