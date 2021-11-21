DROP TABLE dim_clients_hist cascade constraints;
DROP TABLE dim_terminals_hist cascade constraints;
DROP TABLE dim_cards_hist cascade constraints;
DROP TABLE dim_accounts_hist cascade constraints;
DROP TABLE fact_transactions cascade constraints;
DROP TABLE meta_fact_transactions;
DROP TABLE meta_dim_terminals_hist;
DROP TABLE meta_dim_clients_hist;
DROP TABLE meta_dim_accounts_hist;
DROP TABLE meta_dim_cards_hist;
DROP TABLE stg_transactions;
DROP TABLE report cascade constraints;
DROP TABLE meta_report;
/

CREATE TABLE dim_clients_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    client_id CHAR(7),
    last_name VARCHAR(30),
    first_name VARCHAR(30),
    patrinymic VARCHAR(30),
    date_of_birth DATE,
    passport_num CHAR(10),
    passport_valid_to DATE,
    phone CHAR(12),
    start_dt  DATE DEFAULT sysdate,
    end_dt  DATE DEFAULT NULL,
    active NUMBER(1) DEFAULT 1,
    CONSTRAINT samchuk_sv_pk_dim_clients_id_client_id PRIMARY KEY(id)
);
CREATE INDEX samchuk_sv_idx_client_id ON dim_clients_hist(client_id);

CREATE TABLE dim_accounts_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    account_num CHAR(20),
    valid_to DATE,
    client CHAR(7),
    start_dt  DATE DEFAULT sysdate,
    end_dt  DATE DEFAULT NULL,
    active NUMBER(1) DEFAULT 1,
    CONSTRAINT samchuk_sv_dim_accounts_pk_account_num PRIMARY KEY(id)
);
CREATE INDEX samchuk_sv_idx_dim_accounts_hist_account_num ON dim_accounts_hist(account_num);

CREATE TABLE dim_cards_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    card_num CHAR(20),
    account_num CHAR(20),
    start_dt  DATE DEFAULT sysdate,
    end_dt  DATE DEFAULT NULL,
    active NUMBER(1) DEFAULT 1,
    CONSTRAINT samchuk_sv_dim_cards_pk_card_num PRIMARY KEY(id)
);
CREATE INDEX samchuk_sv_idx_dim_cards_hist_card_num ON dim_cards_hist(card_num);

CREATE TABLE dim_terminals_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    terminal_id CHAR(8),
    terminal_type CHAR(3),
    terminal_city VARCHAR(50),
    terminal_address VARCHAR(100),
    start_dt  DATE DEFAULT sysdate,
    end_dt  DATE DEFAULT NULL,
    active NUMBER(1) DEFAULT 1,
    CONSTRAINT samchuk_sv_dim_terminals_hist_pk_id PRIMARY KEY(id)
);
CREATE INDEX samchuk_sv_idx_dim_terminals_hist_terminal_id ON dim_terminals_hist(terminal_id);

CREATE TABLE fact_transactions(
    trans_id NUMBER,
    trans_date DATE,
    card_num CHAR(20),
    oper_type VARCHAR(20),
    amt NUMBER(12, 2),
    oper_result VARCHAR(15),
    terminal CHAR(8)
);
CREATE INDEX samchuk_sv_idx_fact_transactions_card_num ON fact_transactions(card_num);

CREATE GLOBAL TEMPORARY table stg_transactions(
    trans_id      NUMBER NOT NULL,
    trans_date    DATE  NOT NULL,
    card          CHAR(20) NOT NULL,
    account       CHAR(20)  NOT NULL,
    account_valid_to DATE  NOT NULL,
    client        CHAR(7)  NOT NULL,
    last_name     VARCHAR(30)  NOT NULL,
    first_name    VARCHAR(30)  NOT NULL,
    patrinymic    VARCHAR(30),
    date_of_birth DATE  NOT NULL,
    passport      CHAR(10) NOT NULL,
    passport_valid_to DATE NOT NULL,
    phone         CHAR(12) NOT NULL,
    oper_type     VARCHAR(20) NOT NULL,
    amount        NUMBER(12, 2) NOT NULL,
    oper_result   VARCHAR(14) NOT NULL,
    terminal      CHAR(8) NOT NULL,
    terminal_type CHAR(3) NOT NULL,
    city          VARCHAR(50) NOT NULL,
    address       VARCHAR(100) NOT NULL
)ON COMMIT PRESERVE ROWS;

CREATE TABLE meta_fact_transactions(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    last_trans_id NUMBER,
	CONSTRAINT samchuk_sv_pk_meta_fact_transactions_id PRIMARY KEY(id)
);

INSERT INTO meta_fact_transactions(last_trans_id) VALUES(0);

CREATE TABLE meta_dim_terminals_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time    DATE,
    end_time      DATE,
    insert_rows NUMBER,
    update_rows NUMBER,
    create_dt DATE DEFAULT sysdate,
	CONSTRAINT samchuk_sv_pk_meta_dim_terminals_id PRIMARY KEY(id)
);

CREATE TABLE meta_dim_clients_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time    DATE,
    end_time      DATE,
    insert_rows NUMBER,
    update_rows NUMBER,
    create_dt DATE DEFAULT sysdate,
	CONSTRAINT samchuk_sv_pk_meta_dim_clients_id PRIMARY KEY(id)
);

CREATE TABLE meta_dim_accounts_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time    DATE,
    end_time      DATE,
    insert_rows NUMBER,
    update_rows NUMBER,
    create_dt DATE DEFAULT sysdate,
	CONSTRAINT samchuk_sv_pk_meta_dim_accounts_id PRIMARY KEY(id)
);

CREATE TABLE meta_dim_cards_hist(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time    DATE,
    end_time      DATE,
    insert_rows NUMBER,
    update_rows NUMBER,
    create_dt DATE DEFAULT sysdate,
	CONSTRAINT samchuk_sv_pk_meta_dim_cards_id PRIMARY KEY(id)
);

CREATE TABLE report(
     fraud_dt DATE,
     passport CHAR(10),
     fio    VARCHAR(90),
     phone CHAR(13),
     fraud_type VARCHAR(200),
     report_dt DATE
);
CREATE INDEX samchuk_sv_report_idx_fraud_dt ON report(fraud_dt);

CREATE TABLE meta_report(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time    DATE,
    end_time      DATE,
    rowcount NUMBER,
    last_trans_date DATE,
    create_dt DATE DEFAULT sysdate,
	CONSTRAINT samchuk_sv_pk_meta_report_id PRIMARY KEY(id)
);

INSERT INTO meta_report(start_time, end_time, rowcount, last_trans_date)
VALUES(null, null, null, to_date('01.01.2000', 'dd.mm.yyyy'));
/
