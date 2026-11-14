# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.hiring_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.hiring_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.hiring_gold;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.hiring_bronze.raw_files;
