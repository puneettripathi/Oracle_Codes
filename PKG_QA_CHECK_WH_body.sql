create or replace
PACKAGE BODY                         PKG_QA_CHECK_WH AS


   FUNCTION get_tesco_week RETURN VARCHAR2 AS
      tesco_week VARCHAR2(6) := NULL;
   BEGIN
   
      SELECT fis_week_id
        INTO tesco_week
        FROM warehouse_build.date_dim
       WHERE date_id = marketplace.marketplace_etl_pkg.get_current_load_start_date;
   
      RETURN tesco_week;
   
   EXCEPTION
      WHEN OTHERS THEN
         RETURN 'NULL';
   END get_tesco_week;

   -------------------------------------------

   PROCEDURE insert_qa_result(p_etl_layer      VARCHAR2
                             ,p_schema         VARCHAR2
                             ,p_table_name     VARCHAR2
                               ,p_actual_value   VARCHAR2
                             ,p_expected_value VARCHAR2
                             ,p_load_id        NUMBER
                             ,p_tesco_week     VARCHAR2
                             ,p_message        VARCHAR2
                             ,p_run_id         NUMBER := NULL
                             ,p_test_code      VARCHAR2 := NULL
                             ,p_test_result    VARCHAR2 := NULL
                             ,p_commit_yn      VARCHAR2 := 'Y') IS
   BEGIN
      INSERT INTO inbound.qa_result
         (etl_layer
         ,SCHEMA
         ,table_name
         ,actual_value
         ,expected_value
         ,load_id
         ,tesco_week
         ,run_date
         ,message
         ,run_id
         ,test_code
         ,test_result)
      VALUES
         (p_etl_layer
         ,p_schema
         ,p_table_name
         ,p_actual_value
         ,p_expected_value
         ,p_load_id
         ,p_tesco_week
         ,SYSDATE
         ,p_message
         ,p_run_id
         ,p_test_code
         ,p_test_result);
   
      IF upper(p_commit_yn) = 'Y' THEN
         COMMIT;
      END IF;
   
   END insert_qa_result;

   -------------------------------------------

   FUNCTION EXEC_SQL(P_SQL     VARCHAR2
                    ,P_LOAD_ID_OR_CYCLE NUMBER := NULL) RETURN VARCHAR2 IS
                    
      V_RET_VALUE VARCHAR2(100);
      V_REGEXP_COUNT NUMBER;
      V_CURSOR_NAME   INTEGER;
      L_CURSOR SYS_REFCURSOR;
      V_IGNORE   INTEGER;
      
   BEGIN
      IF p_load_id_or_cycle IS NOT NULL AND instr(lower(p_sql), ':load_id') > 0  THEN
      
      EXECUTE IMMEDIATE p_sql
            INTO V_RET_VALUE
            USING p_load_id_or_cycle;
            
      ELSIF   p_load_id_or_cycle IS NOT NULL AND   INSTR(LOWER(P_SQL), ':fis_week_id') > 0 THEN 
      
      SELECT REGEXP_COUNT(P_SQL,'(:fis_week_id)', 1, 'i')  
      INTO v_REGEXP_COUNT  
      FROM DUAL; 
 
      -- Open the cursor and parse the query         
      v_cursor_name := DBMS_SQL.OPEN_CURSOR; 
      DBMS_SQL.PARSE(v_cursor_name, P_SQL, DBMS_SQL.NATIVE);
 
          -- Add bind variables depending on whether they were added to the query.
      FOR I IN 1..V_REGEXP_COUNT
      loop
          DBMS_SQL.BIND_VARIABLE(v_cursor_name, ':fis_week_id', p_load_id_or_cycle);
      end loop;
 
      -- Run the query.
      -- (The return value of DBMS_SQL.EXECUTE for SELECT queries is undefined,
      -- so we must ignore it.)
      V_IGNORE := DBMS_SQL.EXECUTE(V_CURSOR_NAME); 
     l_cursor := DBMS_SQL.TO_REFCURSOR(V_CURSOR_NAME);
    FETCH l_cursor INTO v_ret_value;
      ELSE
         EXECUTE IMMEDIATE p_sql
            INTO v_ret_value;
      END IF;
      RETURN v_ret_value;
   END exec_sql;

   -------------------------------------------

   FUNCTION count_table_recs(p_schema     VARCHAR2
                            ,p_table_name VARCHAR2
                            ,p_load_id    NUMBER := NULL) RETURN NUMBER IS
      v_sql VARCHAR2(1000);
   
   BEGIN
      v_sql := 'SELECT count(*) FROM ' || p_schema || '.' || p_table_name || ' t';
   
      -- NB. No not add load_id where clause to SCD tables as load_id is currently hard-coded to 1 in SCD tables 
      IF p_load_id IS NOT NULL AND p_table_name NOT LIKE '%SCD' THEN
         FOR i IN (SELECT 'x'
                     FROM all_tab_columns
                    WHERE owner = p_schema
                      AND table_name = p_table_name
                      AND column_name = 'LOAD_ID') LOOP
            v_sql := v_sql || ' WHERE load_id = :load_id';
         END LOOP;
      END IF;
   
      RETURN exec_sql(v_sql, p_load_id);
   END count_table_recs;

   -------------------------------------------

   PROCEDURE run_qa_test(p_test_run_id         NUMBER
                        ,P_TEST_REC            INBOUND.QA_TEST%ROWTYPE
                        ,P_WEEK                NUMBER DEFAULT NULL                        
                        ,p_commit_yn           VARCHAR2 := 'Y'
                        ,P_ABORT_ON_FAILURE_YN VARCHAR2 := 'N') IS
      V_LOAD_ID_CYCLE        NUMBER := -1;
      V_LOAD_ID        NUMBER;
      v_tesco_week     VARCHAR2(6);
      v_actual_value   VARCHAR2(100);
      v_expected_value VARCHAR2(100);
      v_test_result    VARCHAR2(10);
      v_message        VARCHAR2(255);
   
   BEGIN
      BEGIN
         
         V_LOAD_ID := MARKETPLACE.MARKETPLACE_ETL_PKG.GET_CURRENT_LOAD_ID;
         
         IF P_WEEK IS NULL  THEN 
         v_load_id_cycle := marketplace.marketplace_etl_pkg.get_current_load_id;
         V_TESCO_WEEK := GET_TESCO_WEEK;
         ELSE
         V_TESCO_WEEK := P_WEEK;
         v_load_id_cycle := P_WEEK;
         END IF;
         
         -- Execute the test SQL or table count
         IF p_test_rec.test_type = 'SQL' THEN
            v_actual_value := exec_sql(p_test_rec.test_sql, v_load_id_cycle);
         ELSIF p_test_rec.test_type = 'FUNCTION' THEN
            v_actual_value := exec_sql('SELECT ' || p_test_rec.test_sql || ' FROM dual');
         ELSIF p_test_rec.test_type = 'COUNT' THEN
            v_actual_value := count_table_recs(p_test_rec.schema, p_test_rec.table_name, v_load_id_cycle);
         ELSE
            raise_application_error(-20001, 'Invalid test type, ' || p_test_rec.test_type);
         END IF;
      
         -- Get the expected result
         IF p_test_rec.expected_type IS NOT NULL THEN
            IF p_test_rec.expected_type = 'SQL' THEN
               v_expected_value := exec_sql(p_test_rec.expected_sql, v_load_id_cycle);
            ELSIF p_test_rec.expected_type = 'FIXED' THEN
               v_expected_value := p_test_rec.expected_value;
            ELSE
               raise_application_error(-20001, 'Invalid expected type, ' || p_test_rec.expected_type);
            END IF;
         
            -- Check actual result matches expected
            IF v_actual_value != v_expected_value THEN
               v_test_result := 'FAIL';
               v_message := 'Actual value, ' || v_actual_value || ', does not match expected, ' || v_expected_value;
            ELSIF p_test_rec.fail_on_zero_yn = 'Y' AND v_actual_value = '0' THEN
               v_test_result := 'FAIL';
               v_message := 'Actual value = ZERO';
            ELSE
               v_test_result := 'PASS';
               v_message := 'OK';
            END IF;
         
            -- no expected result
         ELSE
            v_test_result := NULL;
            v_message := 'Unchecked table count';
         END IF;
      
         insert_qa_result(p_test_rec.etl_layer
                         ,p_test_rec.schema
                         ,p_test_rec.table_name
                         ,v_actual_value
                         ,v_expected_value
                         ,v_load_id
                         ,v_tesco_week
                         ,v_message
                         ,p_test_run_id
                         ,p_test_rec.test_code
                         ,v_test_result
                         ,p_commit_yn);
      
      EXCEPTION
         WHEN OTHERS THEN
            v_test_result := 'ERROR';
            v_message := SQLERRM;
            dbms_output.put_line('Error during QA test execution - ' || SQLERRM);
         
            insert_qa_result(p_test_rec.etl_layer
                            ,p_test_rec.schema
                            ,p_test_rec.table_name
                            ,v_actual_value
                            ,v_expected_value
                            ,v_load_id
                            ,v_tesco_week
                            ,v_message
                            ,p_test_run_id
                            ,p_test_rec.test_code
                            ,v_test_result
                            ,p_commit_yn);
      END;
   
      IF v_test_result != 'PASS' AND upper(p_abort_on_failure_yn) = 'Y' THEN
         raise_application_error(-20000
                                ,'QA Test Failed [test_code=' || p_test_rec.test_code || ', table=' || p_test_rec.schema || '.' || p_test_rec.table_name ||
                                 '] - ' || v_message);
      END IF;
   
   END run_qa_test;

   -------------------------------------------

   PROCEDURE run_qa_test(p_test_code           VARCHAR2
                        ,p_commit_yn           VARCHAR2 := 'Y'
                        ,p_abort_on_failure_yn VARCHAR2 := 'N') IS
      v_test_run_id NUMBER := inbound.seq_qa_test_run.nextval;
   
   BEGIN
      FOR i_test_rec IN (SELECT *
                           FROM inbound.qa_test
                          WHERE test_code LIKE run_qa_test.p_test_code
                            AND UPPER(ENABLED_YN) = 'Y') LOOP
         run_qa_test(p_test_run_id=>v_test_run_id,P_TEST_REC=> i_test_rec, P_COMMIT_YN=>p_commit_yn,P_ABORT_ON_FAILURE_YN=> p_abort_on_failure_yn);
               
      END LOOP;
 
   EXCEPTION
      WHEN no_data_found THEN
         dbms_output.put_line('Invalid test code - ' || p_test_code);
      
   END run_qa_test;

   -------------------------------------------
   PROCEDURE run_qa_tests(p_schema              VARCHAR2
                         ,P_TABLE_NAME          VARCHAR2                         
                         ,p_commit_yn           VARCHAR2 := 'Y'
                         ,p_abort_on_failure_yn VARCHAR2 := 'N') IS
   
      v_counter     NUMBER := 0;
      v_test_rec    inbound.qa_test%ROWTYPE;
      v_test_run_id NUMBER := inbound.seq_qa_test_run.nextval;
   
   BEGIN
      FOR i_test_rec IN (SELECT *
                           FROM inbound.qa_test
                          WHERE SCHEMA = run_qa_tests.p_schema
                            AND table_name = run_qa_tests.p_table_name
                            AND upper(enabled_yn) = 'Y') LOOP
         V_COUNTER := V_COUNTER + 1;
                
         RUN_QA_TEST(p_test_run_id=>V_TEST_RUN_ID,P_TEST_REC=> I_TEST_REC,P_COMMIT_YN=> P_COMMIT_YN, P_ABORT_ON_FAILURE_YN=>P_ABORT_ON_FAILURE_YN);
           
      END LOOP;
   
      -- If there are no tests configured for this table, then run a simple count
      -- (this replicates the old behaviour of qa_proc()
      IF v_counter = 0 THEN
         v_test_rec.schema := p_schema;
         v_test_rec.table_name := p_table_name;
         V_TEST_REC.TEST_TYPE := 'COUNT';
         run_qa_test(p_test_run_id=>v_test_run_id, P_TEST_REC=>v_test_rec,P_COMMIT_YN=> p_commit_yn, P_ABORT_ON_FAILURE_YN=>p_abort_on_failure_yn);
        
      END IF;
   END run_qa_tests;
   
   -------------------------------------------

    PROCEDURE RUN_QA_TESTS(P_SCHEMA              VARCHAR2
                         ,P_TABLE_NAME          VARCHAR2  DEFAULT NULL
                         ,P_WEEK                NUMBER
                         ,P_ETL_LAYER            VARCHAR2 DEFAULT NULL
                         ,p_commit_yn           VARCHAR2 := 'Y'
                         ,p_abort_on_failure_yn VARCHAR2 := 'N') IS
   
      v_counter     NUMBER := 0;
      v_test_rec    inbound.qa_test%ROWTYPE;
      v_test_run_id NUMBER := inbound.seq_qa_test_run.nextval;
   
   BEGIN
      FOR i_test_rec IN (SELECT *
                           FROM inbound.qa_test
                          WHERE SCHEMA = RUN_QA_TESTS.P_SCHEMA
                            AND TABLE_NAME = NVL(RUN_QA_TESTS.P_TABLE_NAME,TABLE_NAME)
                            AND ETL_LAYER= NVL(P_ETL_LAYER,ETL_LAYER)
                            AND UPPER(ENABLED_YN) = 'Y') LOOP
                            
         V_COUNTER := V_COUNTER + 1;    
         RUN_QA_TEST(p_test_run_id=>V_TEST_RUN_ID,P_TEST_REC=> I_TEST_REC,P_WEEK=>P_WEEK, P_COMMIT_YN=>P_COMMIT_YN,P_ABORT_ON_FAILURE_YN=> P_ABORT_ON_FAILURE_YN);
           
      END LOOP;   
   
   END RUN_QA_TESTS;
 
 
   -------------------------------------------
 
   PROCEDURE run_qa_tests(p_etl_layer           VARCHAR2
                         ,p_commit_yn           VARCHAR2 := 'Y'
                         ,p_abort_on_failure_yn VARCHAR2 := 'N') IS
   
      v_test_run_id NUMBER := inbound.seq_qa_test_run.nextval;
   
   BEGIN
      FOR v_test_rec IN (SELECT *
                           FROM inbound.qa_test
                          WHERE etl_layer = run_qa_tests.p_etl_layer
                            AND UPPER(ENABLED_YN) = 'Y'
                            AND SCHEMA<>'WAREHOUSE'
                          ORDER BY SCHEMA
                                  ,TABLE_NAME) LOOP
         run_qa_test(p_test_run_id=>v_test_run_id,P_TEST_REC=> v_test_rec, P_COMMIT_YN=>p_commit_yn, P_ABORT_ON_FAILURE_YN=>p_abort_on_failure_yn);
      END LOOP;
   END run_qa_tests;
 
   -------------------------------------------
 
   PROCEDURE run_all_qa_tests(p_commit_yn           VARCHAR2 := 'Y'
                             ,p_abort_on_failure_yn VARCHAR2 := 'N') IS
   
      v_test_run_id NUMBER := inbound.seq_qa_test_run.nextval;
   
   BEGIN
      FOR v_test_rec IN (SELECT *
                           FROM inbound.qa_test
                          WHERE UPPER(ENABLED_YN) = 'Y'
                          AND SCHEMA<>'WAREHOUSE'
                          ORDER BY etl_layer
                                  ,SCHEMA
                                  ,TABLE_NAME) LOOP
         run_qa_test(p_test_run_id=>v_test_run_id,P_TEST_REC=> v_test_rec, P_COMMIT_YN=>p_commit_yn,P_ABORT_ON_FAILURE_YN=> p_abort_on_failure_yn);
      END LOOP;
   END run_all_qa_tests;

   -------------------------------------------

   /*
   * DEPRICATED - this proc has been superceded by 'run_qa_test' but remain here for backward compatibility (in case it's used anywhere)
   */
   PROCEDURE qa_proc(p_qa_layer  VARCHAR2
                    ,p_qa_schema VARCHAR2
                    ,p_qa_table  VARCHAR2
                    ,p_qa_code   VARCHAR2
                    ,p_commit_yn VARCHAR2) IS
      qa_layer      VARCHAR2(255);
      qa_schema     VARCHAR2(255);
      qa_table      VARCHAR2(255);
      qa_code       VARCHAR2(255);
      qa_tesco_week VARCHAR2(255);
      qa_query      VARCHAR2(255);
      qa_oracle     VARCHAR2(255);
      qa_count      NUMBER := NULL;
      qa_load_id    NUMBER := NULL;
   
   BEGIN
      qa_schema := upper(p_qa_schema);
      qa_layer := upper(p_qa_layer);
      qa_table := upper(p_qa_table);
      qa_code := upper(p_qa_code);
      qa_oracle := 'OK';
   
      qa_load_id := marketplace.marketplace_etl_pkg.get_current_load_id;
      qa_tesco_week := get_tesco_week;
   
      IF qa_layer = 'ET' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1), PDL__CYCLE from ' || qa_schema || '.' || qa_table || ' group by PDL__CYCLE';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count, qa_tesco_week;
      
      ELSIF qa_layer = 'IT' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1), PDL__CYCLE from ' || qa_schema || '.' || qa_table || ' group by PDL__CYCLE';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count, qa_tesco_week;
      
      ELSIF qa_layer = 'IV' THEN
         qa_query := 'select /*+ FULL (t) PARALLEL (t 48) */ count(1) from ' || qa_schema || '.' || qa_table;
         EXECUTE IMMEDIATE qa_query
            INTO qa_count;
      
      ELSIF qa_layer = 'SCD' THEN
         --QA_QUERY  := 'select /*+ PARALLEL (t 48) */ count(1) from ' || QA_SCHEMA || '.' || substr(qa_table, 1, length(qa_table) - 3) || 'DUP';
         --EXECUTE IMMEDIATE QA_QUERY INTO QA_COUNT;
         --insert_qa_result(QA_LAYER, QA_SCHEMA, QA_TABLE_DUP, QA_COUNT, QA_LOAD_ID, QA_TESCO_WEEK, QA_CODE, QA_ORACLE);
      
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1) from ' || qa_schema || '.' || qa_table || ' where current_flag = ''Y''';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count;
      
      ELSIF qa_layer = 'SV' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1) from ' || qa_schema || '.' || qa_table;
         EXECUTE IMMEDIATE qa_query
            INTO qa_count;
      
      ELSIF qa_layer = 'DIM_C' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1) from ' || qa_schema || '.' || qa_table;
         EXECUTE IMMEDIATE qa_query
            INTO qa_count;
      
      ELSIF qa_layer = 'DIM_H' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ count(1) from ' || qa_schema || '.' || qa_table || ' where current_flag = ''Y''';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count;
      
      ELSIF qa_layer = 'FCT' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ NVL(count(1), -1) from ' || qa_schema || '.' || qa_table || ' where LOAD_ID = :QA_LOAD_ID';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count
            USING qa_load_id;
      
      ELSIF qa_layer = 'SEG' THEN
         qa_query := 'select /*+ PARALLEL (t 48) */ NVL(count(1), -1) from ' || qa_schema || '.' || qa_table || ' where LOAD_ID = :QA_LOAD_ID';
         EXECUTE IMMEDIATE qa_query
            INTO qa_count
            USING qa_load_id;
      
      END IF;
   
      insert_qa_result(qa_layer, qa_schema, qa_table, qa_count, null, qa_load_id, qa_tesco_week, qa_oracle, null, qa_code);
   
      IF upper(p_commit_yn) = 'Y' THEN
         COMMIT;
      END IF;
   
   EXCEPTION
      WHEN OTHERS THEN
         qa_oracle := substr(SQLERRM, 1, 254);
         insert_qa_result(qa_layer, qa_schema, qa_table, -1, null, qa_load_id, qa_tesco_week, qa_oracle, null, qa_code);
   END qa_proc;

   -------------------------------------------

   /*
   * DEPRICATED - this proc has been superceded by 'run_qa_test' but remain here for backward compatibility (in case it's used anywhere)
   */
   PROCEDURE run_qa_select(p_qa_table VARCHAR2) IS
      qa_table  VARCHAR2(255);
      qa_count  NUMBER := 0;
      qa_oracle VARCHAR2(255) := 'OK';
   
   BEGIN
      qa_table := upper(p_qa_table);
      FOR x IN (SELECT t_qa_query
                      ,t_qa_code
                      ,t_qa_schema
                      ,t_qa_layer
                  FROM inbound.qa_parameter
                 WHERE t_qa_enabledyn = 'Y'
                   AND t_qa_table = qa_table) LOOP
         EXECUTE IMMEDIATE x.t_qa_query
            INTO qa_count;
      
         insert_qa_result(x.t_qa_layer, x.t_qa_schema, qa_table, qa_count, null, -1, 'NA', qa_oracle, null, x.t_qa_code);
      END LOOP;
   
   END run_qa_select;

END PKG_QA_CHECK_WH;