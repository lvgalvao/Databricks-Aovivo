-- Databricks notebook source
USE SCHEMA bronze;
CREATE OR REFRESH STREAMING LIVE TABLE customers
AS SELECT *
FROM cloud_files(
    "s3a://bucket-cadastro-dos-clientes",
    "json",
    MAP(
        "inferSchema", "true",     -- Deduzir automaticamente o esquema do JSON
        "multiline", "true"        -- Permitir arquivos JSON com várias linhas
    )
);


