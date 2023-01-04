-- Databricks notebook source
SELECT
  customer_id
FROM
  odap_features_customer.features_customer_latest
WHERE
  hypoteky_web_visits_count_in_last_30d > 0
