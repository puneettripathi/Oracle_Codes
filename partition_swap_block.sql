CREATE TABLE "WAREHOUSE_BUILD"."PROMO_ITEM_FCT_SWP"
  (
    "PROMO_ITEM_FID" NUMBER(*,0) NOT NULL ENABLE,
    "PROMO_ID"       NUMBER(*,0) NOT NULL ENABLE,
    "DATE_ID" DATE NOT NULL ENABLE,
    "TIME_START_ID"    NUMBER(*,0) NOT NULL ENABLE,
    "TIME_END_ID"      NUMBER(*,0) NOT NULL ENABLE,
    "PROD_ID"          NUMBER(*,0) NOT NULL ENABLE,
    "PROD_HIST_ID"     NUMBER(*,0) NOT NULL ENABLE,
    "STORE_ID"         NUMBER(*,0) NOT NULL ENABLE,
    "STORE_HIST_ID"    NUMBER(*,0) NOT NULL ENABLE,
    "OFFER_ID"         NUMBER(*,0) NOT NULL ENABLE,
    "PROMO_CYCLE_ID"   NUMBER(*,0) NOT NULL ENABLE,
    "PROMO_DISPLAY_ID" NUMBER(*,0) NOT NULL ENABLE,
    "PROMO_FLYER_ID"   NUMBER(*,0) NOT NULL ENABLE,
    "TRIGGER_FLAG"     CHAR(1 CHAR),
    "REWARD_FLAG"      CHAR(1 CHAR),
    "LOAD_ID"          NUMBER(*,0) NOT NULL ENABLE,
    "CREATED_DTTM" DATE NOT NULL ENABLE,
    "MODIFIED_DTTM" DATE NOT NULL ENABLE,
    CHECK (TRIGGER_FLAG IN ('Y', 'N')) ENABLE,
    CHECK (REWARD_FLAG  IN ('Y', 'N')) ENABLE
  )
  SEGMENT CREATION IMMEDIATE PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 COMPRESS FOR QUERY HIGH LOGGING STORAGE
  (
    INITIAL 4194304 NEXT 4194304 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  )
  TABLESPACE "WH_FCT_QH" ;

ALTER session enable parallel query;
ALTER session enable parallel dml;
ALTER session enable parallel ddl;
ALTER session SET parallel_degree_policy = auto;
SET SERVEROUTPUT ON size unlimited;


DECLARE
  v_partition_name_curr all_tab_partitions.partition_name%type;
  v_partition_name_prev all_tab_partitions.partition_name%type;
  v_partition_value_curr VARCHAR(200);
  v_part_value_curr_date DATE;
  v_partition_value_prev VARCHAR(200);
  v_part_value_prev_date DATE;
  v_partition_position_curr all_tab_partitions.partition_position%type;
  l_partition_has_rows_int PLS_INTEGER;
BEGIN
  FOR prod_stor_agg_part IN



  (SELECT partition_name ,
      partition_position
    FROM all_tab_partitions
    WHERE table_name = 'PROMO_ITEM_FCT'
    AND table_owner  = 'WAREHOUSE'
    ORDER BY partition_position ASC
  )
  LOOP
    v_partition_name_curr  := prod_stor_agg_part.partition_name;
    v_partition_value_curr := get_partition_high_value (p_table_name => 'PROMO_ITEM_FCT', p_table_owner => 'WAREHOUSE', p_partition_name => v_partition_name_curr);



    EXECUTE IMMEDIATE 'SELECT '|| v_partition_value_curr || ' AS return_date FROM dual' INTO v_part_value_curr_date ;
    v_partition_position_curr := prod_stor_agg_part.partition_position;
    BEGIN
      SELECT partition_name
      INTO v_partition_name_prev
      FROM all_tab_partitions
      WHERE table_name       = 'PROMO_ITEM_FCT'
      AND table_owner        = 'WAREHOUSE'
      AND partition_position = v_partition_position_curr - 1
      ORDER BY partition_position ASC;

      v_partition_value_prev := get_partition_high_value (p_table_name => 'PROMO_ITEM_FCT', p_table_owner => 'WAREHOUSE', p_partition_name => v_partition_name_prev);
      EXECUTE IMMEDIATE 'SELECT '|| v_partition_value_prev || ' AS return_date FROM dual' INTO v_part_value_prev_date ;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      /*Deal with the first partition record*/
      v_part_value_prev_date := TO_DATE(' 1800-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN');
    END;




    BEGIN
      SELECT 1
      INTO l_partition_has_rows_int
      FROM WAREHOUSE_BUILD.PT_PROMO_ITEM_FCT_0802
      WHERE DATE_ID >= v_part_value_prev_date
      AND DATE_ID    < v_part_value_curr_date
      AND ROWNUM    <= 1;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      dbms_output.put_line('No data found for partition in warehouse_build ' || v_partition_name_curr ||v_part_value_curr_date||v_part_value_prev_date);
      l_partition_has_rows_int := 0;
    END;
    IF l_partition_has_rows_int = 1 THEN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE WAREHOUSE_BUILD.PROMO_ITEM_FCT_SWP';
      INSERT
        /*+ append */
      INTO WAREHOUSE_BUILD.PROMO_ITEM_FCT_SWP
        (PROMO_ITEM_FID,
    PROMO_ID,
    DATE_ID,
    TIME_START_ID,
    TIME_END_ID,
    PROD_ID,
    PROD_HIST_ID,
    STORE_ID,
    STORE_HIST_ID,
    OFFER_ID,
    PROMO_CYCLE_ID,
    PROMO_DISPLAY_ID,
    PROMO_FLYER_ID,
    TRIGGER_FLAG,
    REWARD_FLAG,
    LOAD_ID,
    CREATED_DTTM,
    MODIFIED_DTTM
        )
      SELECT PROMO_ITEM_FID,
    PROMO_ID,
    DATE_ID,
    TIME_START_ID,
    TIME_END_ID,
    PROD_ID,
    PROD_HIST_ID,
    STORE_ID,
    STORE_HIST_ID,
    OFFER_ID,
    PROMO_CYCLE_ID,
    PROMO_DISPLAY_ID,
    PROMO_FLYER_ID,
    TRIGGER_FLAG,
    REWARD_FLAG,
    LOAD_ID,
    CREATED_DTTM,
    MODIFIED_DTTM

      FROM WAREHOUSE_BUILD.PT_PROMO_ITEM_FCT_0802
      WHERE DATE_ID >= v_part_value_prev_date
      AND DATE_ID    < v_part_value_curr_date ;
      COMMIT;




      EXECUTE IMMEDIATE 'ALTER TABLE WAREHOUSE.PROMO_ITEM_FCT EXCHANGE PARTITION ' || v_partition_name_curr || ' WITH TABLE

WAREHOUSE_BUILD.PROMO_ITEM_FCT_SWP INCLUDING INDEXES WITHOUT VALIDATION';
      dbms_output.put_line('Following partition has been processed '|| v_partition_name_curr);




    END IF;
  END LOOP;
END;
/