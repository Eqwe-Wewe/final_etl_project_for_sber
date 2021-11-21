CREATE OR REPLACE PACKAGE project
AS
    /*
        author:  SAMCHUK_SV
        created: 28.10.2021
        updated: 09.11.2021
        purpose: To complete the final task
    */

PROCEDURE create_report;

END project;

/

CREATE OR REPLACE PACKAGE BODY project
AS

	PROCEDURE insert_into_dim_terminals_hist
	IS
    v_start    DATE;
    v_end      DATE;
    v_insert_rows NUMBER := 0;
    v_update_rows NUMBER := 0;

    CURSOR terminal_cur is
    SELECT terminal,
           terminal_type,
           city,
           address
    FROM stg_transactions;
    
    TYPE terminal_rec_type IS TABLE OF terminal_cur%ROWTYPE;
    terminal_rec            terminal_rec_type;
    terminal_rec_from_dim   terminal_rec_type;

    BEGIN
    v_start := sysdate;

    OPEN terminal_cur;
    FETCH terminal_cur BULK COLLECT INTO terminal_rec;
    CLOSE terminal_cur;
        
    FOR i IN terminal_rec.FIRST..terminal_rec.LAST 
    LOOP
        SELECT terminal_id,
           terminal_type,
           terminal_city,
           terminal_address  
           BULK COLLECT INTO terminal_rec_from_dim
        FROM dim_terminals_hist
        WHERE terminal_id = terminal_rec(i).terminal
        ORDER BY id desc
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

        IF terminal_rec_from_dim.COUNT=0 OR terminal_rec(i).terminal <> terminal_rec_from_dim(1).terminal
            THEN
            INSERT INTO dim_terminals_hist
                  (terminal_id, terminal_type, terminal_city, terminal_address)
            VALUES(
                terminal_rec(i).terminal, terminal_rec(i).terminal_type, terminal_rec(i).city,
                terminal_rec(i).address
            );
            v_insert_rows := v_insert_rows+1;
        ELSIF terminal_rec(i).terminal = terminal_rec_from_dim(1).terminal AND 
           terminal_rec(i).terminal_type <>  terminal_rec_from_dim(1).terminal_type OR
           terminal_rec(i).city <> terminal_rec_from_dim(1).city OR
           terminal_rec(i).address <> terminal_rec_from_dim(1).address
           THEN
               UPDATE dim_terminals_hist SET
               end_dt = sysdate, active = 0
               WHERE end_dt IS NULL AND terminal_id = terminal_rec(i).terminal;
               
               INSERT INTO dim_terminals_hist
                  (terminal_id, terminal_type, terminal_city, terminal_address)
                VALUES(
                    terminal_rec(i).terminal, terminal_rec(i).terminal_type, terminal_rec(i).city,
                    terminal_rec(i).address
                );
                v_update_rows := v_update_rows + 1;
        END IF;
        END LOOP; 
    
        v_end := sysdate;

		INSERT INTO meta_dim_terminals_hist
		(start_time, end_time, insert_rows, update_rows)
		VALUES (v_start, v_end, v_insert_rows, v_update_rows);

		--COMMIT;

	END insert_into_dim_terminals_hist;


	PROCEDURE insert_into_dim_clients_hist
	IS
    v_start    DATE;
    v_end      DATE;
    v_insert_rows NUMBER := 0;
    v_update_rows NUMBER := 0;

    CURSOR client_cur is
    SELECT client,
           last_name,
           first_name,
           patrinymic,
           date_of_birth,
           passport,
           passport_valid_to,
           phone
    FROM stg_transactions;
    
    TYPE client_rec_type IS TABLE OF client_cur%ROWTYPE;
    client_rec            client_rec_type;
    client_rec_from_dim   client_rec_type;

    BEGIN
    v_start := sysdate;

    OPEN client_cur;
    FETCH client_cur BULK COLLECT INTO client_rec;
    CLOSE client_cur;
        
    FOR i IN client_rec.FIRST..client_rec.LAST 
    LOOP
        SELECT client_id,
           last_name,
           first_name,
           patrinymic,
           date_of_birth,
           passport_num,
           passport_valid_to,
           phone
           BULK COLLECT INTO client_rec_from_dim
        FROM dim_clients_hist
        WHERE client_id = client_rec(i).client
        ORDER BY id desc
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

        IF client_rec_from_dim.COUNT=0 OR client_rec(i).client <> client_rec_from_dim(1).client
            THEN
            INSERT INTO dim_clients_hist
            (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone)
            VALUES(
                client_rec(i).client, client_rec(i).last_name, client_rec(i).first_name,
                client_rec(i).patrinymic, client_rec(i).date_of_birth, client_rec(i).passport,
                client_rec(i).passport_valid_to, client_rec(i).phone
            );
            v_insert_rows := v_insert_rows+1;
        ELSIF client_rec(i).client = client_rec_from_dim(1).client AND 
           client_rec(i).last_name <>  client_rec_from_dim(1).last_name OR
           client_rec(i).first_name <> client_rec_from_dim(1).first_name OR
           client_rec(i).patrinymic <> client_rec_from_dim(1).patrinymic OR
           client_rec(i).date_of_birth <> client_rec_from_dim(1).date_of_birth OR
           client_rec(i).passport <> client_rec_from_dim(1).passport OR
           client_rec(i).passport_valid_to <> client_rec_from_dim(1).passport_valid_to OR
           client_rec(i).phone <> client_rec_from_dim(1).phone
           THEN
               UPDATE dim_clients_hist SET
               end_dt = sysdate, active = 0
               WHERE end_dt IS NULL AND client_id = client_rec(i).client;
               
               INSERT INTO dim_clients_hist
                   (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone)
               VALUES(
                    client_rec(i).client, client_rec(i).last_name, client_rec(i).first_name,
                    client_rec(i).patrinymic, client_rec(i).date_of_birth, client_rec(i).passport,
                    client_rec(i).passport_valid_to, client_rec(i).phone
                );
                v_update_rows := v_update_rows + 1;
        END IF;
        END LOOP; 
    
        v_end := sysdate;

		INSERT INTO meta_dim_clients_hist
		(start_time, end_time, insert_rows, update_rows)
		VALUES (v_start, v_end, v_insert_rows, v_update_rows);

		--COMMIT;

	END insert_into_dim_clients_hist;


	PROCEDURE insert_into_dim_accounts_hist
	IS
    v_start    DATE;
    v_end      DATE;
    v_insert_rows NUMBER := 0;
    v_update_rows NUMBER := 0;

    CURSOR account_cur is
    SELECT account,
           account_valid_to,
           client
    FROM stg_transactions;
    
    TYPE account_rec_type IS TABLE OF account_cur%ROWTYPE;
    account_rec            account_rec_type;
    account_rec_from_dim   account_rec_type;

    BEGIN
    v_start := sysdate;

    OPEN account_cur;
    FETCH account_cur BULK COLLECT INTO account_rec;
    CLOSE account_cur;
        
    FOR i IN account_rec.FIRST..account_rec.LAST 
    LOOP
        SELECT account_num,
               valid_to,
               client
               BULK COLLECT INTO account_rec_from_dim
        FROM dim_accounts_hist
        WHERE account_num = account_rec(i).account
        ORDER BY id desc
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

        IF account_rec_from_dim.COUNT=0 OR account_rec(i).account <> account_rec_from_dim(1).account
            THEN
            INSERT INTO dim_accounts_hist(account_num, valid_to, client)
            VALUES(account_rec(i).account, account_rec(i).account_valid_to, account_rec(i).client);

            v_insert_rows := v_insert_rows+1;
        ELSIF account_rec(i).account = account_rec_from_dim(1).account AND 
           account_rec(i).account_valid_to <>  account_rec_from_dim(1).account_valid_to OR
           account_rec(i).client <> account_rec_from_dim(1).client
           THEN
               UPDATE dim_accounts_hist SET
               end_dt = sysdate, active = 0
               WHERE end_dt IS NULL AND account_num = account_rec(i).account;
               
               INSERT INTO dim_accounts_hist(account_num, valid_to, client)
               VALUES(account_rec(i).account, account_rec(i).account_valid_to, account_rec(i).client);
               
                v_update_rows := v_update_rows + 1;
        END IF;
        END LOOP; 
    
        v_end := sysdate;

		INSERT INTO meta_dim_accounts_hist
		(start_time, end_time, insert_rows, update_rows)
		VALUES (v_start, v_end, v_insert_rows, v_update_rows);

		--COMMIT;
	END insert_into_dim_accounts_hist;


	PROCEDURE insert_into_dim_cards_hist
	IS
    v_start    DATE;
    v_end      DATE;
    v_insert_rows NUMBER := 0;
    v_update_rows NUMBER := 0;

    CURSOR card_cur is
    SELECT card,
           account
    FROM stg_transactions;
    
    TYPE card_rec_type IS TABLE OF card_cur%ROWTYPE;
    card_rec            card_rec_type;
    card_rec_from_dim   card_rec_type;

    BEGIN
    v_start := sysdate;

    OPEN card_cur;
    FETCH card_cur BULK COLLECT INTO card_rec;
    CLOSE card_cur;
        
    FOR i IN card_rec.FIRST..card_rec.LAST 
    LOOP
        SELECT card_num,
               account_num
               BULK COLLECT INTO card_rec_from_dim
        FROM dim_cards_hist
        WHERE card_num = card_rec(i).card
        ORDER BY id desc
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

        IF card_rec_from_dim.COUNT=0 OR card_rec(i).card<> card_rec_from_dim(1).card
            THEN
            INSERT INTO dim_cards_hist(card_num, account_num)
            VALUES(card_rec(i).card, card_rec(i).account);

            v_insert_rows := v_insert_rows+1;
        ELSIF card_rec(i).card = card_rec_from_dim(1).card AND 
           card_rec(i).account <>  card_rec_from_dim(1).account 
           THEN
               UPDATE dim_cards_hist SET
               end_dt = sysdate, active = 0
               WHERE end_dt IS NULL AND card_num = card_rec(i).card;
               
               INSERT INTO dim_cards_hist(card_num, account_num)
               VALUES(card_rec(i).card, card_rec(i).account);
               
                v_update_rows := v_update_rows + 1;
        END IF;
        END LOOP; 
    
        v_end := sysdate;

		INSERT INTO meta_dim_cards_hist
		(start_time, end_time, insert_rows, update_rows)
		VALUES (v_start, v_end, v_insert_rows, v_update_rows);

		--COMMIT;

	END insert_into_dim_cards_hist;


	PROCEDURE insert_into_fact_transactions
	IS

	v_last_trans_id NUMBER;

	BEGIN

		SELECT last_trans_id
			   INTO v_last_trans_id
		FROM   meta_fact_transactions
		ORDER BY 1 DESC
		OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

		INSERT INTO fact_transactions
		SELECT trans_id,
			   trans_date,
			   card,
			   oper_type,
			   amount,
			   oper_result,
			   terminal
		FROM stg_transactions
		WHERE trans_id > v_last_trans_id;

		INSERT INTO meta_fact_transactions
		(last_trans_id)
		SELECT trans_id
		FROM   fact_transactions
		ORDER BY 1 desc
		OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

		--COMMIT;

	END insert_into_fact_transactions;


	PROCEDURE insert_into_report
	IS
	v_start    DATE;
	v_end      DATE;
	v_rowcount NUMBER;
	v_last_trans_date DATE;
	v_last_fraud_dt DATE;

	BEGIN
		v_start := sysdate;


		SELECT last_trans_date
			   INTO v_last_trans_date
		FROM   meta_report
		ORDER BY 1 DESC
		OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

		INSERT INTO report
		WITH tbl as(
		SELECT t.trans_id,
			   t.trans_date,
			   --to_char(t.trans_date, 'hh24:mi:ss') as time,
			   lag(t.trans_date) over(partition by cl.client_id order by t.trans_date) dt_lag,
			   --lag (to_char(t.trans_date, 'hh24:mi:ss')) over(partition by cl.client_id order by t.trans_date) time_lag,
			   cl.last_name||' '||cl.first_name||' '||cl.patrinymic as fio,
			   cl.client_id,
			   cl.passport_num,
			   cl.passport_valid_to,
			   a.valid_to,
			   cl.phone,
			   ter.terminal_city,
			   lag(ter.terminal_city) over(partition by cl.client_id order by t.trans_date) as last_city
			   --last_value(t.trans_id) over(partition by trunc(t.trans_date) order by cl.client_id) as last_val
		from fact_transactions t
			 JOIN dim_cards_hist c
			 ON t.card_num = c.card_num
			 JOIN dim_accounts_hist a
			 ON c.account_num = a.account_num
			 JOIN dim_clients_hist cl
			 ON a.client = cl.client_id
			 JOIN dim_terminals_hist ter
			 ON t.terminal = ter.terminal_id
		order by cl.client_id, t.trans_date
		),
		tbl2 as(
		SELECT tbl.*,
			   CASE
				   WHEN trans_date > passport_valid_to THEN '1'
				   ELSE NULL
			   END fraud_1,
			   CASE
				   WHEN trans_date > valid_to THEN '2'
				   ELSE NULL
			  END fraud_2,
			  CASE
				  WHEN trans_date - dt_lag <= 1/24 AND terminal_city <> last_city THEN '3'
				  ELSE NULL
			  END fraud_3
		from tbl
		),
		tbl3 as(
		SELECT trans_id,
			   trans_date,
			   passport_num,
			   fio,
			   client_id,
			   phone,
			   CASE
				   WHEN fraud_1||fraud_2||fraud_3 = '1'
					   THEN 'Совершение операции при просроченном паспорте'
				   WHEN fraud_1||fraud_2||fraud_3 = '12'
					   THEN  'Совершение операции при просроченном паспорте и недействующем договоре'
				   WHEN fraud_1||fraud_2||fraud_3 = '123'
					   THEN 'Совершение операции при просроченном паспорте, недействующем договоре и в разных городах в течение 1 часа'
				   WHEN fraud_1||fraud_2||fraud_3 = '2'
					   THEN 'Совершение операции при недействующем договоре'
					WHEN fraud_1||fraud_2||fraud_3 = '23'
					   THEN 'Совершение операции при недействующем договоре и в разных городах в течение 1 часа'
					WHEN fraud_1||fraud_2||fraud_3 = '3'
					   THEN 'Совершение операции в разных городах в течение 1 часа'
					WHEN fraud_1||fraud_2||fraud_3 = '13'
					   THEN 'Совершение операции при просроченном паспорте и в разных городах в течение 1 часа'
					ELSE null
				END as fraud_type,
				sysdate as report_date
		from tbl2
		WHERE fraud_1 IS NOT NULL
			  OR fraud_2 IS NOT NULL
			  OR fraud_3 IS NOT NULL
		),tbl4 as(
		SELECT last_value(trans_id) over(partition by trunc(trans_date) order by client_id) as last_id,
			   tbl3.*
		FROM tbl3
		ORDER BY trans_date
		)
		SELECT trans_date,
			   passport_num,
			   fio,
               '+7'||REGEXP_SUBSTR(phone,'(\d{10})\s*$'),
			   fraud_type,
			   sysdate
		FROM   tbl4
		WHERE  1=1
			   AND trans_id = last_id
			   AND trans_date > TRUNC(v_last_trans_date) +1;

		--COMMIT;

		SELECT fraud_dt
			  INTO v_last_fraud_dt
		FROM  report
		ORDER BY 1 desc
		OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

		v_end := sysdate;
		v_rowcount := SQL%rowcount;

		INSERT INTO meta_report
		(start_time, end_time, rowcount, last_trans_date)
		VALUES (v_start, v_end, v_rowcount, v_last_fraud_dt);

		--COMMIT;

	END insert_into_report;

    
	PROCEDURE create_report
	IS 
	BEGIN
		project.insert_into_dim_terminals_hist;
		project.insert_into_dim_clients_hist;
		project.insert_into_dim_accounts_hist;
		project.insert_into_dim_cards_hist;
		project.insert_into_fact_transactions;
		project.insert_into_report;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            dbms_output.put_line('ЧТО-ТО ПОШЛО НЕ ТАК');
            RAISE;
	END create_report;

END project;
/
