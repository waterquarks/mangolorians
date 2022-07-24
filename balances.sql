--
-- PostgreSQL database dump
--

-- Dumped from database version 12.11 (Ubuntu 12.11-0ubuntu0.20.04.1)
-- Dumped by pg_dump version 14.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: balances; Type: TABLE; Schema: public; Owner: ioaquine
--

CREATE TABLE public.balances (
    asset text NOT NULL,
    account text NOT NULL,
    deposits numeric,
    borrows numeric,
    unsettled numeric,
    net numeric,
    value numeric,
    equity numeric,
    assets numeric,
    liabilities numeric,
    leverage numeric,
    init_health_ratio numeric,
    maint_ealth_ratio numeric,
    "timestamp" timestamp with time zone NOT NULL,
    market_percentage_move_to_liquidation numeric
);


ALTER TABLE public.balances OWNER TO ioaquine;

--
-- Name: balances balances_pkey; Type: CONSTRAINT; Schema: public; Owner: ioaquine
--

ALTER TABLE ONLY public.balances
    ADD CONSTRAINT balances_pkey PRIMARY KEY (asset, account, "timestamp");


--
-- Name: balances_asset_timestamp_idx; Type: INDEX; Schema: public; Owner: ioaquine
--

CREATE INDEX balances_asset_timestamp_idx ON public.balances USING btree (asset, "timestamp");


--
-- PostgreSQL database dump complete
--

