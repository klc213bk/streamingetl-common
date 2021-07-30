create table T_LOGMINER_SCN
(
	streaming_name VARCHAR2(30),
	prev_scn NUMBER(19, 0),
	scn NUMBER(19, 0),
	scn_insert_time TIMESTAMP,
	scn_update_time TIMESTAMP,
	health_time TIMESTAMP,
	primary key (streaming_name)
)
