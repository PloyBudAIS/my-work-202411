#! /bin/ksh
#################################################################################
# Project      : All                            
# Programmer   : Rungaroon J.            
# Process name : cm_shell_job_interface_multi_file.sh
# Description  : Main Shell Call Process by Config
# Remark       : TEXT/Dat/Sync File 
# 30/03/2016   : New Release By Rungaroon J.     
# 27/06/2017   : Add funtion insertlog , update job status , edit Function UpdateParameter  By Amphai S.
# [3]: 18/10/2017 : By Pakawadee S. : Fix bug subport search date of file include numeric [5.3.2.2 Check Row/Size of DAT File : DATE_CHECK]    
# [4]: 28/04/2020 : By Sho H. : Fix REC_CODE = 8 does not return error to control-m
# [5]: xx/11/2024 : By Budsakorn W. : Support FILE_CHECK = L (list .dat) and X (have not .sync)
#################################################################################

############# 1.Globals Config ############# 
GLO_PATH=oper
GLO_CONNECT_DB=connect_opercnm

PARAM_PGM_NAME=""
LOG_MESSAGE=""
PGM_ID=""

########## 2. Assign Variables #############
### Check parameter [Program name] from command shell  ###
LOG_MESSAGE="## Check parameter [Program name] from command shell ..."
echo ${LOG_MESSAGE}
PARAM_PGM_NAME=$1

if [ -n "${PARAM_PGM_NAME}" ]; then
    PGM_ID=${PARAM_PGM_NAME}
    LOG_MESSAGE="Parameter [Program name] is : $PGM_ID"
    echo ${LOG_MESSAGE}
else
    LOG_MESSAGE="Parameter [Program name] is null"
    echo ${LOG_MESSAGE}
    #InsertError "$SCRIPT_NAME" "$MESSAGE" $RET_CD_CON_M "$DATA_SOURCE"
    exit 1;
fi

#CM_USERID=/users/kritsap8/rungaroj/pwd/${GLO_CONNECT_DB}.sql; export CM_USERID
CM_USERID=/users/${GLO_PATH}/pwd/${GLO_CONNECT_DB}.sql; export CM_USERID
SCRIPT_PATH=/users/${GLO_PATH}/cmprocess; export SCRIPT_PATH
LOG_PATH=/users/${GLO_PATH}/cmlog; export LOG_PATH
TMP_FILE=/users/${GLO_PATH}/cmrun/${PGM_ID}.tmp; export TMP_FILE
OUT_FILE=/users/${GLO_PATH}/cmrun/${PGM_ID}.out; export OUT_FILE
LST_FILE=/users/${GLO_PATH}/cmrun/${PGM_ID}.lst; export LST_FILE
LST_CHK=/users/${GLO_PATH}/cmrun/${PGM_ID}.lsc; export LST_CHK
LST_SUB=/users/${GLO_PATH}/cmrun/${PGM_ID}.lsb; export LST_SUB
OUT_CHK=/users/${GLO_PATH}/cmrun/${PGM_ID}_chk.out; export OUT_CHK
TMP_CHK=/users/${GLO_PATH}/cmrun/${PGM_ID}.tmc; export TMP_CHK
TMP_SPLIT=/users/${GLO_PATH}/cmrun/${PGM_ID}_tmp.spl; export TMP_SPLIT
OUT_SPLIT=/users/${GLO_PATH}/cmrun/${PGM_ID}_out.spl; export OUT_SPLIT
DATA_BK_PATH=/users/${GLO_PATH}/cmdata/BACKUP; export DATA_BK_PATH
FILE_Y=/users/${GLO_PATH}/cmrun/${PGM_ID}.fy; export FILE_Y
DATA_TMP_FILE=/users/${GLO_PATH}/cmrun/${PGM_ID}_data.tmp; export DATA_TMP_FILE
TMP_FILE99=/users/${GLO_PATH}/cmrun/${PGM_ID}.tmp99; export TMP_FILE99

RESULT=""
D=`date +%Y%m%d`
DT=`date +%Y%m%d_%H%M%S`
LOG_FILE=${PGM_ID}'.log.'${DT}

SUCCESS_FLG="N"
DATE_FROM=""
DATE_TO=""
LAST_FILE_NAME=""
UPD_LAST_FILE_NAME=""
FILE_PATH=""
FILE_NAME=""
FILE_TYPE=""
FILE_CHECK=""
GET_FILE=""
SCRIPT_NAME=""
DELAY_FLG=""
SPLIT_FLG=""
SPLIT_ROW=""
SPLIT_PATH=""
SUBFOLDER_FLG=""
SUBFOLDER_FORMAT=""
JOB_NAME=""

JOB_RESULT=""
JOB_REAULT_FLG=""
VER_PARA=""
COMPLETE_FLG="Y"
DAT_ROW_SIZE=""
SYNC_ROW_SIZE=""
DATA_SYNC=""
DAT_FILE_NAME=""
SYNC_ROW_SIZE=""
FILE_EXISTS=""
CURRENT_DATA=""
FILE_FOR_SQL=""
DATE_COUNT=""
DATE_CHECK=""
FILE_DATE_FLG=""
COUNT=""
DATE_FOR_SQL=""
V_DATEFROM=""
V_DATETO=""
ALL_FILE_FLG="N"
ALL_FILE_FLG_SPILT="N"
JOB_STATUS=""
POSITION_DATE=""   

##START Function InsertError 
InsertError(){
LOG_MESSAGE="## Insert CM_Etl_Error..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
v_script_name=$1
v_msg_error=$2
v_short_key="CONTROL_M_RESULT : "$3
v_data_source=$4
v_get_file=$5


sqlplus -s @${CM_USERID} ${PGM_ID} "$v_short_key" "$v_msg_error" "$v_script_name" "" "$v_data_source" > $TMP_FILE <<EOF
set echo off verify off feed off head off line 1000 pages 0
INSERT INTO cm_etl_error
(seq_nbr, job_name   , run_date, short_key, message,script_name , data_source )
VALUES
(cm_seq_err_log.nextval  , '&&1'    , SYSDATE ,'&&2'      , '&&3'  ,'&&4'     ,'&&6');
COMMIT;
EOF


LOG_MESSAGE="## Insert CM_Etl_Log..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
echo "DATE_FROM : "$DATE_FROM
echo "DATE_TO : "$DATE_TO
echo "message : "$2
echo "date : "${DT}
echo "get_file : "$5
echo "script_name : "$v_script_name



sqlplus -s @${CM_USERID} ${PGM_ID} ${DT} ${DATE_FROM} ${DATE_TO} "$v_msg_error" "$v_script_name" "$v_get_file"> $DATA_TMP_FILE <<EOF
set echo off verify off feed off head off line 1000 pages 0
INSERT INTO cm_etl_log
(seq_nbr, job_name, run_date,finish_date,input_par1, input_par2, remark,script_name)
VALUES
(cm_seq_err_log.nextval
,'&&1'
,to_date('&&2','YYYYMMDD_HH24MISS') 
,sysdate
,decode('&&3',00000000,null,decode('&&7','F',to_char(to_date('&&3','yyyymmdd')+1, 'dd/mm/yyyy'),to_char(to_date('&&3','yyyymmdd'), 'dd/mm/yyyy')))
,decode('&&4',00000000,null,to_char(to_date('&&4', 'yyyymmdd'), 'dd/mm/yyyy'))
,'&&5'
,'&&6');
COMMIT;
EOF


} 
##END FUNCTION InsertError

##START Function UpdateParameter
UpdateParameter(){

LOG_MESSAGE="## Update CM_Etl_Parameter ##"
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

v_input_par=$1
echo "v_input_par  : "$1

v_last_file_name=${UPD_LAST_FILE_NAME}
v_delay_flag=${DELAY_FLG}
v_all_file_flag=${ALL_FILE_FLG}
v_success_flg=$(echo $v_input_par| cut -d'|' -f 1)
v_date_from=$(echo $v_input_par| cut -d'|' -f 2)
v_date_to=$(echo $v_input_par| cut -d'|' -f 3)
v_result=$(echo $v_input_par| cut -d'|' -f 4)
v_short_key=$(echo $v_input_par| cut -d'|' -f 5)
v_msg=$(echo $v_input_par| cut -d'|' -f 6)
v_data_source=$(echo $v_input_par| cut -d'|' -f 7)
v_get_file=$(echo $v_input_par| cut -d'|' -f 8)


echo "Success Flag     : "$v_success_flg
echo "Date from        : "$v_date_from
echo "Date to          : "$v_date_to
echo "Result           : "$v_result
echo "File data name   : "$v_last_file_name
echo "All File Flag    : "$v_all_file_flag
echo "Delay Flag       : "$v_delay_flag
echo "Short Key        : "$v_short_key
echo "Message          : "$v_msg
echo "Data Source      : "$v_data_source
echo "SCRIPT_NAME      : "$SCRIPT_NAME
echo "Get_file         : "$v_get_file

##Not Success insert cm_etl_error
if [ $v_success_flg == "N" ]; then
	InsertError "$SCRIPT_NAME" "$v_msg" "$v_result" "$v_data_source" "$v_get_file"
	 JOB_STATUS="F"
	else 
	 JOB_STATUS="S"
fi

echo "JOB_STATUS : "${JOB_STATUS}

if [ ${v_delay_flag} == "N" ]; then
  if [ ${v_success_flg} == "Y" ]; then
sqlplus -s @${CM_USERID} ${DT} ${PGM_ID} "$v_date_from" "$v_date_to" "$v_result" "$v_last_file_name" "$v_success_flg" "$JOB_STATUS" > $DATA_TMP_FILE <<EOF
set echo off verify off feed off head off line 1000 pages 0
		UPDATE cm_etl_parameter p
		SET input_par1  = decode(p.get_file,'D',to_char( to_date('&&3', 'yyyymmdd') + decode('&&7','Y',1,0), 'dd/mm/yyyy'),to_char( to_date('&&3', 'yyyymmdd'),'dd/mm/yyyy')),
			input_par2  = decode(p.get_file,'D',to_char( to_date('&&4', 'yyyymmdd') + 1, 'dd/mm/yyyy'),decode('&&8','F',to_char(to_date('&&4', 'yyyymmdd')+1, 'dd/mm/yyyy'),to_char(to_date('&&3', 'yyyymmdd'), 'dd/mm/yyyy'))),
			status_flag = '&&5',
			finish_date = SYSDATE,
			last_file_name = nvl(decode('&&6','NO',null,'',null,'&&6'),last_file_name),
			job_status = '&&8'
		 WHERE job_name ='&&2';
		COMMIT;

		select p.input_par1 || '|' ||
		p.input_par2 || '|' ||
		p.last_file_name
		from cm_etl_parameter p
		where job_name ='&&2';
		
EOF
else 
  if [ ${v_result} == "1" ]; then

sqlplus -s @${CM_USERID} "$PGM_ID" "$v_result" ${JOB_STATUS} > $DATA_TMP_FILE <<EOF
        set echo off verify off feed off head off line 1000 pages 0
        UPDATE cm_etl_parameter
           SET finish_date = SYSDATE,
               status_flag = '&&2',
			   job_status = '&&3'
        WHERE job_name ='&&1';
		COMMIT;
			
EOF
  else

sqlplus -s @${CM_USERID} ${DT} "$PGM_ID" "$v_result" ${JOB_STATUS} "$v_date_from" "$v_date_to" > $DATA_TMP_FILE <<EOF
        set echo off verify off feed off head off line 1000 pages 0
        UPDATE cm_etl_parameter 
           SET input_par2  = decode(get_file,'D',to_char( to_date('&&6','yyyymmdd')+1, 'dd/mm/yyyy'),to_char(to_date('&&5', 'yyyymmdd'), 'dd/mm/yyyy')),
		       finish_date = SYSDATE,
               status_flag = '&&3',
			   job_status = '&&4'
        WHERE job_name ='&&2';
		COMMIT;
			
EOF
  fi
fi

else 
##Update Load Delay
    if [ $v_success_flg == "Y" ]; then
    ## Command In Loop ##
	##if [ ${v_all_file_flag} == "Y" ]; then
    sqlplus -s @${CM_USERID} ${DT} "$PGM_ID" "$v_result" ${JOB_STATUS} > $DATA_TMP_FILE <<EOF
    set echo off verify off feed off head off line 1000 pages 0
        UPDATE cm_lov_master
           SET value1  = 'Y',
             last_upd_by = user,
             last_upd_date = sysdate
        WHERE LOV_TYPE ='LOAD_BY_CURSOR' 
        AND lov_subtype ='&&2'
        AND active_flg ='Y'
        AND value1 ='N';

        UPDATE cm_etl_parameter
           SET DELAY_FLG  = 'N',
           DELAY_DATE_FROM  = NULL,
           DELAY_DATE_TO  = NULL,
           DELAY_FILE =NULL,
           finish_date = SYSDATE,
           status_flag = '&&3',
		   job_status = '&&4'
         WHERE job_name ='&&2';
        COMMIT;
EOF
        ############# 7. End Process #############
        LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
        echo ${LOG_MESSAGE}
        echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

	    
			exit ${v_result};
	##fi
    
    else
    ## Command In Loop ##
    sqlplus -s @${CM_USERID} ${DT} "$PGM_ID" "$v_result" ${JOB_STATUS} > $DATA_TMP_FILE <<EOF
        set echo off verify off feed off head off line 1000 pages 0
        UPDATE cm_etl_parameter
           SET finish_date = SYSDATE,
               status_flag = '&&3',
			   job_status = '&&4'
        WHERE job_name ='&&2';
        COMMIT;
EOF
	    
        ############# 7. End Process #############
        LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
        echo ${LOG_MESSAGE}
        echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
    		
        exit ${v_result};
    fi 
fi

NEXT_DATE_FROM=`cat $DATA_TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $1}'`
NEXT_DATE_TO=`cat $DATA_TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $2}'`
THE_LAST_FILE_NAME=`cat $DATA_TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $3}'`

LOG_MESSAGE="Updated Parameter..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

LOG_MESSAGE="INPUT_PAR1:"${NEXT_DATE_FROM}
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

LOG_MESSAGE="INPUT_PAR2:"${NEXT_DATE_TO}
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

LOG_MESSAGE="LAST_FILE_NAME:"${THE_LAST_FILE_NAME}
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}


if [ ${v_success_flg} == "Y" ]; then
  if [ ${v_all_file_flag} == "Y" ]; then
  ############# 7. End Process #############
  LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
  echo ${LOG_MESSAGE}
  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

    exit 0;
  else
    LOG_MESSAGE="==================== Next File ===================="
    echo ${LOG_MESSAGE}
    echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
  fi
else
############# 7. End Process #############
  LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
  echo ${LOG_MESSAGE}
  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
  exit 1;
fi
}
##END Function UpdateParameter

VerifyParameter(){
echo "VER_PARA :"${VER_PARA}
if [ ${VER_PARA} == "N" ]; then
    JOB_RESULT=1
	SHORT_KEY="VERIFY_PARAMETER"
	DATA_SOURCE="CM_ETL_PARAMETER"
	UpdateParameter "$VER_PARA|$DATE_FROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE" 
fi
}
##END Function VerifyParameter

PrepareFileAndLoad(){
	if [ $DELAY_FLG == "N" ]; then
		LOG_MESSAGE="CHECK DATE FILE ... "
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
		#DATE_CHECK=$(echo ${FILE_FOR_SQL} | sed -e s/[^0-9]//g | awk '{ printf "%s\n", substr($1,1,8) }')
					DATE_CHECK=$(echo ${FILE_FOR_SQL} |awk '{ printf "%s\n", substr($1,'${POSITION_DATE}',8) }')
		LOG_MESSAGE="DATE_CHECK ... "${DATE_CHECK}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

		##First row
		if [ -z "$DATE_COUNT" ];then 
			if [ $GET_FILE == "F" ]; then
				DATE_COUNT=$DATE_CHECK
			else
				DATE_COUNT=$V_DATEFROM
			fi
			
			if [ $DATE_COUNT == $DATE_CHECK ]; then
				LOG_MESSAGE="START DATE_COUNT ... "${DATE_COUNT}
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			else
				LOG_MESSAGE="Some file missing ..."
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		
				#LOG_MESSAGE="## The File Date : "${DATE_COUNT}" is missing ... "
				#echo ${LOG_MESSAGE}
				#echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

				JOB_RESULT_FLG="N"				
				JOB_RESULT=4
				DATA_SOURCE=$CURRENT_DATA
				if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ]; then
					V_DATEFROM=$DATE_COUNT
				fi
				UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
			fi
		else
			LOG_MESSAGE="DATE_COUNT ... "${DATE_COUNT}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
		fi

		##RANG FILE 
		## Command In Loop ##
		sqlplus -s @${CM_USERID} ${DATE_CHECK} ${DATE_COUNT} > $DATA_TMP_FILE <<EOF
		set echo off verify off feed off head off line 1000 pages 0
		SELECT trim(to_date('&&1','yyyymmdd') - to_date('&&2','yyyymmdd'))
		FROM dual;
EOF

		COUNT=`cat $DATA_TMP_FILE | awk '{print $1}'`	

		if [ $COUNT -ge 2 ]; then
			FILE_DATE_FLG="N"	
			LOG_MESSAGE="##Some file missing ... "
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			## Command In Loop ##
			sqlplus -s @${CM_USERID} ${PGM_ID} > $TMP_FILE <<EOF
			set echo off verify off feed off head off line 1000 pages 0
			SELECT to_char(t.date_count,'yyyymmdd')
			FROM 
			(SELECT nvl((SELECT to_date(substr(MAX(data_source), length(p.file_name)+1, 8), 'yyyymmdd') + 1
											FROM cm_etl_log
											WHERE job_name = p.job_name
											AND substr(data_source,-4,4)=p.file_type
											AND finish_date IS NOT NULL
											), sysdate) date_count
					FROM cm_etl_parameter p
					WHERE p.job_name='&&1')t;
EOF
			DATE_COUNT=`cat $TMP_FILE | awk '{print $1}'`

			LOG_MESSAGE="## The File Date : "${DATE_COUNT}" is missing ... "
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		else
			DATE_COUNT=$DATE_CHECK
			FILE_DATE_FLG="Y"
			LOG_MESSAGE="DATE_COUNT ... "${DATE_COUNT}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		fi

	else
		FILE_DATE_FLG="Y"
	fi 


	#check Date Not Pass
	if [ $FILE_DATE_FLG == "N" ]; then 
		LOG_MESSAGE="Program : Some file missing ..."
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		JOB_RESULT_FLG="N"
		JOB_RESULT=4
		DATA_SOURCE=$CURRENT_DATA
		if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ]; then
			V_DATEFROM=$DATE_COUNT
		fi
		UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
	else	
		### 5.4 Check Split File before Call PL	
		if [ $SPLIT_FLG == "Y" ]; then
			### 5.4.1 IF SPLIT_FLG = Y  ### 
			LOG_MESSAGE="## SPLIT_FLG = Y .... "
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			
			### 5.4.1.1 Copy file for backup  ### 
			LOG_MESSAGE="## Copy File: "${FILE_FOR_SQL}
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			cp ${FILE_FOR_SQL} ${DATA_BK_PATH}
			LOG_MESSAGE="## Splitting File..."
			# echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

			ls ${FILE_FOR_SQL} > $TMP_SPLIT
			rows_split=`cat ${TMP_SPLIT}|wc -l|sed 's/ //g'`

			num_split=1
			while [ $num_split -le $rows_split ]
			do
				tail_row_split="tail +${num_split} ${TMP_SPLIT}"
				$tail_row_split > $OUT_SPLIT

				filename_split=`head -1 $OUT_SPLIT | cut -f1`
				LOG_MESSAGE="Split File Name: "${filename_split}
				echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

				split -l ${SPLIT_ROW} ${filename_split} ${filename_split}
				rm ${filename_split}

				let num_split=$num_split+1
			done

			### 5.4.1.2 Load File ## 
			LOG_MESSAGE="## Loading File..."
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

			ls ${FILE_FOR_SQL}*|sort > $TMP_SPLIT
			rows_split=`cat ${TMP_SPLIT}|wc -l|sed 's/ //g'`
		

			LOG_MESSAGE="Count Splitted File: "${rows_split}
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			
			##Start Check Rows Split 	
			if [ $rows_split -eq 0 ]; then
				LOG_MESSAGE="Splitted file not found."
				filename_split="NO"
				JOB_RESULT_FLG="N"
				JOB_RESULT=2
				if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ] ; then					    
					V_DATEFROM=DATE_COUNT		
				fi
				if [ $ALL_FILE_FLG_SPILT == "Y" ] && [ $ALL_FILE_FLG == "Y" ]; then
					ALL_FILE_FLG="Y"
				else
					ALL_FILE_FLG="N"
				fi		
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
			else 
				num_split=1
				while [ $num_split -le $rows_split ]
				do
					tail_row_split="tail +${num_split} ${TMP_SPLIT}"
					$tail_row_split > $OUT_SPLIT
					filename_split=`head -1 $OUT_SPLIT | cut -f1`
					if [ $num_split -eq $rows_split ]; then
					ALL_FILE_FLG_SPILT="Y"
					else
					ALL_FILE_FLG_SPILT="N"
					fi
					echo "ALL_FILE_FLG_SPILT : "$ALL_FILE_FLG_SPILT
					echo "ALL_FILE_FLG : "$ALL_FILE_FLG	  
					### 5.4.1.3 Check Log Datasource
					if [ $DELAY_FLG == "N" ]; then
						## Command In Loop ##
						sqlplus -s @${CM_USERID} ${PGM_ID} ${filename_split} > $FILE_Y <<EOF
						set echo off verify off feed off head off line 1000 pages 0
						select 'Y'
						from cm_etl_log l
						where l.job_name='&&1'
						and l.data_source='&&2'
						and l.finish_date is not null
						and rownum=1;
EOF
						rows_y=`cat ${FILE_Y}|wc -l|sed 's/ //g'`
						if [ $rows_y -ne 0 ]; then
							FILE_Y="Y"
							LOG_MESSAGE="## File : "${filename_split}" is exists in log data source"
							#echo ${LOG_MESSAGE}
							echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	

						else
							FILE_Y="N"
							LOG_MESSAGE="## File : "${filename_split}" is not exists in log data source"
							#echo ${LOG_MESSAGE}
							echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
						fi
					else
						FILE_Y="N"
						LOG_MESSAGE="## This delay case don't check log data source"
						#echo ${LOG_MESSAGE}
						echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					fi  ###Check Log Datasource
					
					#FILE_Y NOT IN CM_ETL_LOG
					if [ $FILE_Y == "N" ]; then					
			
						### 5.4.1.4 Execut Query ## 			  
						LOG_MESSAGE="## Execute Query ... ["${num_split}"] : "${filename_split}
						#echo ${LOG_MESSAGE}
						echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}   
							
						#DATE_FOR_SQL=$(echo ${filename_split} | sed -e s/[^0-9]//g | awk '{ printf "%s\n", substr($1,1,8) }')
												DATE_FOR_SQL=$(echo ${filename_split}	|awk '{ printf "%s\n", substr($1,'${POSITION_DATE}',8) }')						  

##Excute Process.sql In Loop ##
						sqlplus -s @${CM_USERID} >> ${LOG_PATH}/${LOG_FILE} <<EOF
						@${SCRIPT_PATH}/${SCRIPT_NAME} ${SPLIT_PATH} ${filename_split} ${DATE_FROM} ${DATE_TO} ${PGM_ID}
						##${DATE_FOR_SQL}
EOF
						RESULT=$?;
						## 5.4.1.5 Check Result ##
						DATA_SOURCE=$filename_split
						###V_DATEFROM=DATE_FROM
						JOB_RESULT=""
						ALL_FILE_FLG=""
						if [ $ALL_FILE_FLG_SPILT == "Y" ] && [ $ALL_FILE_FLG == "Y" ]; then
							ALL_FILE_FLG="Y"
						else
							ALL_FILE_FLG="N"
						fi
									
						if [ $RESULT -eq 16 ]; then
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Execute Error / Data Error : Return ("${RESULT}")"
							filename_split="NO"
							JOB_RESULT=5
						elif [ $RESULT -eq 4 ]; then
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Can not open data file : Return ("${RESULT}")"
							filename_split="NO"
							JOB_RESULT=5
							JOB_RESULT_FLG="N"
						## [4]
						elif [ $RESULT -eq 8 ]; then
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Conflicted columns did not match with file : Return ("${RESULT}")"
							filename_split="NO"
							JOB_RESULT=6
							JOB_RESULT_FLG="N"		
						elif [ $RESULT -eq 10 ]; then
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Maintain Log Error : Return ("${RESULT}")"
							JOB_RESULT=0
							JOB_RESULT_FLG="Y"
						elif [ $RESULT -eq 12 ]; then
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Check cm_etl_error : Return ("${RESULT}")"
							filename_split="NO"
							JOB_RESULT=0
							JOB_RESULT_FLG="Y"
						else
							LOG_MESSAGE="## "${SCRIPT_NAME}" : Execute Completely : Return ("${RESULT}")"
							JOB_RESULT=0
							JOB_RESULT_FLG="Y"
						fi;  
						
						if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ]; then
							if [ $JOB_RESULT -eq 0 ]; then
								V_DATEFROM=$DATE_CHECK	
							else
								V_DATEFROM=$DATE_COUNT
								
								UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"
							fi
						fi
						
						echo ${LOG_MESSAGE}
						echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
										
					else
						LOG_MESSAGE="## Skip file ..."
						echo ${LOG_MESSAGE}
						echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					fi 

					let num_split=$num_split+1 	
					
				done 
			fi
			##End Check Rows Split 		
							
		fi ### END 5.4 Check Split File before Call PL	

		if [ $SPLIT_FLG == "N" ]; then 	
			### 5.4.2 IF SPLIT_FLG = N  ###	
			LOG_MESSAGE="## SPLIT_FLG = N .... "
			#echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					
			### 5.4.2.1 Check Log Datasource
			if [ $DELAY_FLG == "N" ]; then
				LOG_MESSAGE="Check Log Datasource ...."
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				## Command In Loop ##
				sqlplus -s @${CM_USERID} ${PGM_ID} ${FILE_FOR_SQL} > $FILE_Y <<EOF
				set echo off verify off feed off head off line 1000 pages 0
				select 'Y'
				from cm_etl_log l
				where l.job_name='&&1'
				and l.data_source='&&2'
				and l.finish_date is not null
				and rownum=1;
EOF
				rows_y=`cat ${FILE_Y}|wc -l|sed 's/ //g'`

				if [ $rows_y -ne 0 ]; then
					FILE_Y="Y"
					LOG_MESSAGE="## File : "${FILE_FOR_SQL}" is exists in log data source"
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	

				else
					FILE_Y="N"
					LOG_MESSAGE="## File : "${FILE_FOR_SQL}" is not exists in log data source"
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				fi

			else
				FILE_Y="N"
				LOG_MESSAGE="## This delay case don't check log data source"
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			fi  ###Check Log Datasource
					

			if [ $FILE_Y == "N" ]; then			
				### 5.4.2. IF Execute Query ###
				LOG_MESSAGE="## Execute Query ... ["${num}"] : "${FILE_FOR_SQL}
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}  
					
				#DATE_FOR_SQL=$(echo ${FILE_FOR_SQL} | sed -e s/[^0-9]//g | awk '{ printf "%s\n", substr($1,1,8) }')
								DATE_FOR_SQL=$(echo ${FILE_FOR_SQL} |awk '{ printf "%s\n", substr($1,'${POSITION_DATE}',8) }')					## Command In Loop ##
				sqlplus -s @${CM_USERID} >> ${LOG_PATH}/${LOG_FILE} <<EOF
				@${SCRIPT_PATH}/${SCRIPT_NAME} ${FILE_PATH} ${FILE_FOR_SQL} ${DATE_FOR_SQL} ${DATE_FOR_SQL} ${PGM_ID}
				#${DATE_FOR_SQL}
EOF
				RESULT=$?;
				## 5.4.2.2 Check Result ##

				if [ $RESULT -eq 16 ]; then
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Execute Error / Error : Return ("${RESULT}")"
					JOB_RESULT=5
					JOB_RESULT_FLG="N"
				elif [ $RESULT -eq 4 ]; then
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Can not open data file : Return ("${RESULT}")"
					JOB_RESULT=5
					JOB_RESULT_FLG="N"	
				## [4]
				elif [ $RESULT -eq 8 ]; then
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Conflicted columns did not match with file : Return ("${RESULT}")"
					JOB_RESULT=6
					JOB_RESULT_FLG="N"
				elif [ $RESULT -eq 10 ]; then
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Maintain Log Error : Return ("${RESULT}")"
					FILE_FOR_SQL="NO"
					JOB_RESULT=0
					JOB_RESULT_FLG="Y"
				elif [ $RESULT -eq 12 ]; then
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Check cm_etl_error : Return ("${RESULT}")"	
					JOB_RESULT=0
					JOB_RESULT_FLG="Y"
				else
					LOG_MESSAGE="## "${FILE_FOR_SQL}" : Execute Completely : Return ("${RESULT}")"
					JOB_RESULT=0
					JOB_RESULT_FLG="Y"	
				fi; 
			
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}   

				if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ]; then
					if [ $RESULT -eq 0 ]; then
						V_DATEFROM=$DATE_CHECK
					else
						V_DATEFROM=$DATE_COUNT
						UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
					fi

				fi
						


			else
				LOG_MESSAGE="## Skip file ..."
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			fi
				
		fi  ##SPLIT FLG = N
#### ploy			
	fi	#END check Date Not Pass
}
##END Function PrepareFileAndLoad

CheckFile(){
		############# 5.2 Check Total File discover #############
    if [ $DELAY_FLG == "N" ] || [ $DELAY_FLG == "D" ]; then
		LOG_MESSAGE="Finding file...Date from : "${V_DATEFROM}" ...Date to : "${V_DATETO}" ..."		
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		ls ${FILE_NAME}*.*| awk '"'${FILE_NAME}'""'${V_DATEFROM}'" <= $1 {print $1}' | awk '"'${FILE_NAME}'""'${V_DATETO}'""_999999">= $1 {print $1}' |sort > $TMP_FILE  	
	elif [ $DELAY_FLG == "F" ]; then
	    # rechect .dat have command 
		LAST_FILE_NAME=$(echo $LAST_FILE_NAME | cut -f 1 -d '.')
		LOG_MESSAGE="Finding file... "${LAST_FILE_NAME}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		ls ${LAST_FILE_NAME}|sort > $TMP_FILE 
	else
		LOG_MESSAGE="DELAY_FLG is not [F] or [D] or [N]."
		#echo ${LOG_MESSAGE}
		#echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		JOB_RESULT_FLG="N"
		JOB_RESULT=1
		UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
	fi
	
	if [ $FILE_TYPE == ".dat" ]; then
		if [ $FILE_CHECK == "X" ]; then
			rowl=`cat ${TMP_FILE} |wc -l|sed 's/ //g'`	
			DATA_SOURCE="$FILE_NAME$V_DATEFROM*.dat"	
		else
			ls ${FILE_NAME}*.sync| awk '"'${FILE_NAME}'""'${V_DATEFROM}'" <= $1 {print $1}' | awk '"'${FILE_NAME}'""'${V_DATETO}'""_999999">= $1 {print $1}' |sort > $LST_FILE  
			rowl=`cat ${LST_FILE} |wc -l|sed 's/ //g'`
	    	DATA_SOURCE="$FILE_NAME$V_DATEFROM*.sync"
		fi	
	else
        rowl=`cat ${TMP_FILE} |wc -l|sed 's/ //g'`	
		DATA_SOURCE="$FILE_NAME$V_DATEFROM*$FILE_TYPE"
	fi

	echo "data_source :"$DATA_SOURCE
	echo ${LOG_MESSAGE}
	echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
	
	############# 5.2.2 Check Total File .sync #############
	echo "file rows:"$rowl
    if [ $rowl -eq 0 ]; then
		
		
		if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "F" ]; then
			JOB_RESULT_FLG="Y"
			JOB_RESULT=0
			LOG_MESSAGE="## "${FILE_FOR_SQL}" : Execute Completely : Return ("${RESULT}")"
		else
			if [ $FILE_TYPE == ".txt" ] || [ $FILE_TYPE == ".csv" ]; then
				LOG_MESSAGE="File ${FILE_TYPE} not found."	
			elif [ $FILE_TYPE == ".dat" ] && [ $FILE_CHECK == "X" ]; then
				LOG_MESSAGE="File .dat not found."
			else
				LOG_MESSAGE="File .sync not found."	
			fi	
			JOB_RESULT_FLG="N"
			JOB_RESULT=2
		fi
		
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
	else
	    if [ $SPLIT_FLG == "Y" ]; then
			LOG_MESSAGE="Total list file data = "${rowl}
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	
			#### 5.2.1.1 Copy File and Change Permission #####
			LOG_MESSAGE="Copy File To: ("${SPLIT_PATH}")"
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

			cp `cat ${TMP_FILE}` ${SPLIT_PATH} 
			cd ${SPLIT_PATH}
			chmod 777 `cat ${TMP_FILE}`
			
		else
			LOG_MESSAGE="Total list file data = "${rowl}
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		fi
    fi

	############# 5.3 Check File .txt OR .csv OR .dat/.sync #############
	LOG_MESSAGE="## Check File..."
	echo ${LOG_MESSAGE}
	echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	## Start Loop Check
	num=1
	
	while [ $num -le $rowl ]
	do  
		if [ $FILE_TYPE == ".txt" ] || [ $FILE_TYPE == ".csv" ]; then	
			tail_row="tail +${num} ${TMP_FILE}"
			$tail_row > $OUT_FILE
		elif [ $FILE_TYPE == ".dat" ] && [ $FILE_CHECK == "X" ]; then
			tail_row="tail +${num} ${TMP_FILE}"
			$tail_row > $OUT_FILE
		else  
			tail_row="tail +${num} ${LST_FILE}"
			$tail_row > $OUT_FILE
		fi   
		
   	    CURRENT_DATA=`head -1 $OUT_FILE | cut -f1`
      	DATA_SOURCE=$CURRENT_DATA

		if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "D" ]; then
			#V_DATEFROM=$(echo ${CURRENT_DATA} | sed -e s/[^0-9]//g | awk '{ printf "%s\n", substr($1,1,8) }')
            V_DATEFROM=$(echo ${CURRENT_DATA} |awk '{ printf "%s\n", substr($1,'${POSITION_DATE}',8) }')
		fi
		
		if [ $num -eq $rowl ]; then
			ALL_FILE_FLG="Y"
		else
			ALL_FILE_FLG="N"
		fi
	
		LOG_MESSAGE="######################### File number ["${num}"]:["${CURRENT_DATA}"] #########################"
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  
		if [ $FILE_TYPE == ".txt" ] || [ $FILE_TYPE == ".csv" ]; then	
			############# 5.3.1 Check File .txt or .csv #############
			LOG_MESSAGE="## File ${FILE_TYPE} : ${CURRENT_DATA} ..."
			FILE_FOR_SQL=${CURRENT_DATA}		
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			
			echo "File: "${FILE_FOR_SQL}
			PrepareFileAndLoad
		
		elif [ $FILE_TYPE == ".dat" ] && [ $FILE_CHECK == "X" ]; then
			############# 5.3.2 Check File .dat and  in have not .sync #############
			LOG_MESSAGE="## File .dat : "${CURRENT_DATA}" ..."
			FILE_FOR_SQL=${CURRENT_DATA}		
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			
			echo "File: "${FILE_FOR_SQL}
			PrepareFileAndLoad
		else
			############# 5.3.3 Check File .dat/.sync #############
			if [ $FILE_CHECK == "L" ]; then
				##while IFS= read -r SYNC_FILE_NAME; do
					##SYNC_FILE="$FILE_PATH/$SYNC_FILE_NAME"
					##if [ -f "$SYNC_FILE" ]; then
						##echo "Processing .sync file: $SYNC_FILE"
						##LOG_MESSAGE="## Check Data : list file .dat ..."${CURRENT_DATA}
						##echo ${LOG_MESSAGE}
						##echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
						#########
						while IFS= read -r DAT_FILE_NAME; do
							DAT_FILE=$DAT_FILE_NAME
							if [ -f "$DAT_FILE" ]; then
								LOG_MESSAGE="## check list .dat File found: ${DAT_FILE} ..."
								echo ${LOG_MESSAGE}
								echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								
								LOG_MESSAGE="## File ${FILE_TYPE} : ${DAT_FILE} ..."
								FILE_FOR_SQL=${DAT_FILE}		
								echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								
								echo "File: "${FILE_FOR_SQL}
								PrepareFileAndLoad
							else
								LOG_MESSAGE="## check list .dat File not found: ${DAT_FILE} ..."
								echo ${LOG_MESSAGE}
								echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								LOG_MESSAGE="File .dat not found."
								echo ${LOG_MESSAGE}
								echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
								JOB_RESULT_FLG="N"
								JOB_RESULT=2
								UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"
							fi
						done < "$CURRENT_DATA"
					##else
						##LOG_MESSAGE="## .sync File not found: ${SYNC_FILE} ..."
						##echo "${LOG_MESSAGE}" >> "$LOG_PATH/$LOG_FILE"
					##fi
				##done < "$TMP_FILE"

			else
			##### case .dat/.sync in case FILE CHECK row or size #####
				LOG_MESSAGE="## Check Data : file .sync ..."${CURRENT_DATA}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				cat ${CURRENT_DATA} > ${TMP_CHK}

				DAT_FILE_NAME=`cat ${CURRENT_DATA}|nawk -F"|" '{print $1}' `
				SYNC_ROW_SIZE=`cat ${CURRENT_DATA}|nawk -F"|" '{print $2}' `
				SYNC_DATA=`cat ${CURRENT_DATA} `
				echo "DAT_FILE_NAME : "${DAT_FILE_NAME}
				echo "SYNC_ROW_SIZE : "${SYNC_ROW_SIZE}
				
				if [ -z ${SYNC_DATA} ]; then
					JOB_RESULT_FLG="N"
					JOB_RESULT=3
					LOG_MESSAGE="No data in file .sync"
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}				
					UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
				fi
					
				LOG_MESSAGE="## Check Name File .sync and Data Name file .dat in .sync file"
				
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				
				chk_s=${CURRENT_DATA%%.*}
				chk_d=${DAT_FILE_NAME%%.*}
				LOG_MESSAGE="Name File .sync : "${chk_s}
				
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				LOG_MESSAGE="Data Name file .dat in .sync file : "${chk_d}
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				
			
				if [ $chk_s == $chk_d ]; then
					LOG_MESSAGE="Name File .sync == Data Name file .dat in .sync file"	
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				else
					LOG_MESSAGE="Name File .sync <> Data Name file .dat in .sync file"
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	  
					JOB_RESULT_FLG="N"
					JOB_RESULT=3
					UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"		
				fi
				
				
				LOG_MESSAGE="## File .sync : Finding ..."${DAT_FILE_NAME}" : Size or Row ..."${SYNC_ROW_SIZE}
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					
				##### 5.3.2.1 find .dat #####
				echo "----------------------------------"$DAT_FILE_NAME
				
				ls ${DAT_FILE_NAME} > $LST_CHK
				row_d=`cat ${LST_CHK} |wc -l|sed 's/ //g'`

				DATA_SOURCE=${DAT_FILE_NAME}

				if [ $row_d -eq 0 ]; then
					LOG_MESSAGE="File .dat not found."
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
					JOB_RESULT_FLG="N"
					JOB_RESULT=2
					UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
				else
					LOG_MESSAGE="Total list file .dat = "${row_d}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					#echo ${LOG_MESSAGE}
				fi

				
				##### 5.3.2.2 Check Row/Size of DAT File #####			
				if [ $FILE_CHECK == "R" ]; then
					DAT_ROW_SIZE=`cat ${DAT_FILE_NAME} |wc -l|sed 's/ //g'`
					DAT_ROW_SIZE=$(($DAT_ROW_SIZE))
				else
					DAT_ROW_SIZE=`ls -l ${DAT_FILE_NAME} | awk '{ print $5}'`
				fi
				
				LOG_MESSAGE="File .dat ["${num}"] : "${DAT_FILE_NAME}" | Size or Row : "${DAT_ROW_SIZE}
				#echo ${LOG_MESSAGE}
				echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				
				if [ $SYNC_ROW_SIZE -ne $DAT_ROW_SIZE ]; then
					LOG_MESSAGE="Data file .dat/.sync is wrong row or size."
					#echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}		
					JOB_RESULT_FLG="N"
					JOB_RESULT=3
					UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
				else
					FILE_FOR_SQL=${DAT_FILE_NAME}	
					##UPD_LAST_FILE_NAME=${DAT_FILE_NAME}
					echo "File: "${FILE_FOR_SQL}
					PrepareFileAndLoad				
				fi
			fi			
					
		fi ###CHK TXT DAT

#######END Validate File JOB_RESULT = 2, 3################


	if [ $JOB_RESULT_FLG == "Y" ]; then
		UPD_LAST_FILE_NAME=${FILE_FOR_SQL}	
	fi
 	
 
let num=$num+1
done	
}
##END Function CheckFile

############# 3. Start Process #############
LOG_MESSAGE="==================== START TIME "`date +%d/%m/%Y_%H:%M:%S`" ====================="
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} > $LOG_PATH/${LOG_FILE}

LOG_MESSAGE="Log File: "${LOG_PATH}/${LOG_FILE}
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

############# 4. Get Parameter #############
LOG_MESSAGE="## Getting Parameter..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

##### 4.1 Get: Input Parameter
sqlplus -s @${CM_USERID} ${PGM_ID} > $TMP_FILE <<EOF
set echo off verify off feed off head off line 2000 pages 0
SELECT decode(t.date_from,null,'00000000',to_char(t.date_from,'yyyymmdd'))|| '|' ||
	   decode(t.date_to,null,'00000000',to_char(t.date_to,'yyyymmdd'))|| '|' ||
       t.last_file_name || '|' ||
       t.file_path || '|' ||
       t.file_name || '|' ||
       t.file_type || '|' ||
       t.file_check || '|' ||
       t.get_file || '|' ||
       t.script_name || '|' ||
       t.delay_flg || '|' ||
       t.split_flg || '|' ||
       t.split_row || '|' ||
       t.split_path || '|' ||
       case when substr(t.sub_folder,1,1) = '\' then 
             'Y'|| '|' ||substr(t.sub_folder,2)|| '|' ||to_char(nvl(t.date_to,sysdate),substr(t.sub_folder,2))
            else 'N'|| '|' ||t.sub_folder|| '|' ||t.sub_folder
       end  
	   || '|' ||t.job_name
  FROM (SELECT CASE 
	         WHEN p.delay_flg = 'N' THEN CASE WHEN p.get_file = 'D' 
						  THEN  to_date(p.input_par1, 'dd/mm/yyyy')
						  ELSE nvl(to_date(substr(REGEXP_SUBSTR(p.last_file_name,'[^\.]+') , length(p.file_name)+1, 8), 'yyyymmdd') , sysdate -1) END 
                WHEN p.delay_flg = 'D' THEN
                  p.delay_date_from
                WHEN p.delay_flg = 'F' THEN
   				 nvl(to_date(substr(REGEXP_SUBSTR(p.Delay_File,'[^\.]+') , length(p.file_name)+1, 8), 'yyyymmdd'),sysdate) 
				ELSE  
				to_date(p.input_par1, 'dd/mm/yyyy')   
				END AS date_from,
         CASE 
	     WHEN p.delay_flg = 'N' THEN  CASE WHEN p.get_file = 'D' 
					      THEN to_date(p.input_par2, 'dd/mm/yyyy')
					      ELSE sysdate END 
            WHEN p.delay_flg = 'D' THEN
                  p.delay_date_to
            WHEN p.delay_flg = 'F' THEN
                  nvl(to_date(substr(REGEXP_SUBSTR(p.Delay_File,'[^\.]+') , length(p.file_name)+1, 8), 'yyyymmdd'),sysdate) 
          ELSE   
		  to_date(p.input_par2, 'dd/mm/yyyy') 
		  END AS date_to,
          CASE 
             WHEN p.delay_flg = 'N' THEN nvl(p.last_file_name,p.file_name) ELSE nvl(p.delay_file,p.file_name) END AS last_file_name
               ,p.file_path
               ,p.file_name
               ,p.file_type
               ,p.file_check
               ,p.get_file
               ,p.script_name
               ,p.delay_flg
               ,nvl(p.split_flg,'N') split_flg
               ,nvl(p.split_row,0) split_row
               ,nvl(p.split_path,'NULL') split_path
               ,p.sub_folder
	        ,p.job_name
              FROM cm_etl_parameter p
              WHERE p.job_name = '&&1') t;

EOF

DATE_FROM=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $1}'`
DATE_TO=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $2}'`
LAST_FILE_NAME=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $3}'`
DELAY_FILE=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $3}'`
FILE_PATH=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $4}'`
MFILE_PATH=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $4}'`
FILE_NAME=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $5}'`
FILE_TYPE=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $6}'`
FILE_CHECK=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $7}'`
GET_FILE=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $8}'`
SCRIPT_NAME=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $9}'`
DELAY_FLG=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $10}'`
SPLIT_FLG=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $11}'`
SPLIT_ROW=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $12}'`
SPLIT_PATH=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $13}'`
SUBFOLDER_FLG=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $14}'`
SUBFOLDER_FORMAT=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $15}'`
SUMFOLDER_END=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $16}'`
JOB_NAME=`cat $TMP_FILE | awk 'BEGIN { FS=OFS="|" } {print $17}'`

echo "DATE_FROM          :"${DATE_FROM}
echo "DATE_TO            :"${DATE_TO}          
echo "LAST_FILE_NAME     :"${LAST_FILE_NAME}
##echo "DELAY_FILE	     :"${DELAY_FILE}       
echo "FILE_PATH          :"${FILE_PATH}          
echo "MFILE_PATH         :"${MFILE_PATH}           
echo "FILE_NAME          :"${FILE_NAME}           
echo "FILE_TYPE          :"${FILE_TYPE}          
echo "FILE_CHECK         :"${FILE_CHECK}           
echo "GET_FILE           :"${GET_FILE}         
echo "SCRIPT_NAME        :"${SCRIPT_NAME}          
echo "DELAY_FLG          :"${DELAY_FLG}           
echo "SPLIT_FLG          :"${SPLIT_FLG}           
echo "SPLIT_ROW          :"${SPLIT_ROW}          
echo "SPLIT_PATH         :"${SPLIT_PATH}       
echo "SUBFOLDER_FLG      :"${SUBFOLDER_FLG}        
echo "SUBFOLDER_FORMAT   :"${SUBFOLDER_FROM}  
echo "SUBFOLDER_END   	 :"${SUBFOLDER_END}       
echo "JOB_NAME           :"${JOB_NAME}          

#2
##### 4.1.2 Update Parameter Running #####
LOG_MESSAGE="## Update Parameter Running..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
JOB_STATUS="R"

sqlplus -s @${CM_USERID} ${PGM_ID} ${JOB_STATUS} ${DT} > $TMP_FILE <<EOF
set echo off verify off feed off head off line 1000 pages 0
UPDATE cm_etl_parameter
   SET  start_date = to_date('&&3','YYYYMMDD_HH24MISS'),
		job_status = '&&2'
 WHERE job_name ='&&1';
COMMIT;
EOF

#2  

##### 4.2 Verify Parameter #####
echo " " >> $LOG_PATH/${LOG_FILE}
LOG_MESSAGE="## Verify Parameter..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

if [ -n "${JOB_NAME}" ]; then
      LOG_MESSAGE="JOB_NAME = $JOB_NAME"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
else
      LOG_MESSAGE="JOB_NAME is wrong"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  SCRIPT_NAME="cm_shell_job_interface_multi"
	  RET_CD_CON_M="1"
	  DATA_SOURCE="CM_ETL_PARAMETER"
	  InsertError "$SCRIPT_NAME" "$MESSAGE" "$RET_CD_CON_M" "$DATA_SOURCE"
      exit 1;
fi

VER_PARA="Y"

if [ -n "${DELAY_FLG}" ]; then
      LOG_MESSAGE="DELAY_FLG= $DELAY_FLG"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  
	   if [ $DELAY_FLG == "N" ]; then
       ##### 4.2.1 Verify Parameter Normal Case #####
		  LOG_MESSAGE="## Parameter Normal Case..."
		  echo ${LOG_MESSAGE}
		  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  
			if [ -n "${GET_FILE}" ]; then
				  LOG_MESSAGE="  GET_FILE= $GET_FILE"
				  echo ${LOG_MESSAGE}
				  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				  
						if [ $GET_FILE == "F" ]; then	  
								if [ -n "${LAST_FILE_NAME}" ]; then
									  LOG_MESSAGE="LAST_FILE_NAME= $LAST_FILE_NAME"
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								else
									  LOG_MESSAGE="LAST_FILE_NAME is NULL or empty."
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}									  
									  VER_PARA="N"
									  VerifyParameter
								fi
						elif [ $GET_FILE == "D" ]; then
								if [ -n "${DATE_FROM}" ]; then
								    if [ ${DATE_FROM} == "00000000" ]; then
									  LOG_MESSAGE="DATE_FROM is NULL or empty."
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
									  VER_PARA="N"
									  VerifyParameter
									else 
									
									  LOG_MESSAGE="  DATE_FROM= $DATE_FROM"
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
									fi
									  
								else
									  LOG_MESSAGE="DATE_FROM is NULL or empty."
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
									  VER_PARA="N"
									  VerifyParameter
								fi

								if [ -n "${DATE_TO}" ]; then
								  if [ ${DATE_TO} == "00000000" ]; then
								   LOG_MESSAGE="DATE_TO is NULL or empty."
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
									  VER_PARA="N"
									  VerifyParameter
								  else 
								   LOG_MESSAGE="DATE_TO= $DATE_TO"
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								  
								  fi 
								else
									  LOG_MESSAGE="DATE_TO is NULL or empty."
									  echo ${LOG_MESSAGE}
									  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
									  VER_PARA="N"
									  VerifyParameter
								fi
						else
								LOG_MESSAGE="GET_FILE is not [F] or [D]."
								echo ${LOG_MESSAGE}
								echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
								VER_PARA="N"
								VerifyParameter
						fi				  
			else
				  LOG_MESSAGE="GET_FILE is NULL or empty."
				  echo ${LOG_MESSAGE}
				  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				  VER_PARA="N"
				  VerifyParameter
			fi

		elif [ $DELAY_FLG == "F" ]; then
		##### 4.2.2.1 Verify Parameter Delay Case #####
			  LOG_MESSAGE="## Parameter Delay Case ..."
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

					if [ -n "${DELAY_FILE}" ]; then
						  LOG_MESSAGE="DELAY_FILE= $DELAY_FILE"
						  echo ${LOG_MESSAGE}
						  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					else
						  LOG_MESSAGE="DELAY_FILE is NULL or empty."
						  echo ${LOG_MESSAGE}
						  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
						  VER_PARA="N"
						  VerifyParameter
					fi
		elif [ $DELAY_FLG == "D" ]; then
		##### 4.2.2.2 Verify Parameter Delay Case #####
			  LOG_MESSAGE="##Parameter Delay Case ..."
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			  
				if [ -n "${DATE_FROM}" ]; then
				     if [ ${DATE_FROM} == "00000000" ]; then
					   LOG_MESSAGE="DELAY_DATE_FROM is NULL or empty."
					   echo ${LOG_MESSAGE}
					   echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					   VER_PARA="N"
					   VerifyParameter
					 else 
					 
					 LOG_MESSAGE="DELAY_DATE_FROM= $DATE_FROM"
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					 
					 
					 fi
					  
				else
					  LOG_MESSAGE="DELAY_DATE_FROM is NULL or empty."
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					  VER_PARA="N"
					  VerifyParameter
				fi


				if [ -n "${DATE_TO}" ]; then
				  if [ ${DATE_TO} == "00000000" ]; then
				    LOG_MESSAGE="DELAY_DATE_TO is NULL or empty."
					echo ${LOG_MESSAGE}
					echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					VER_PARA="N"
					  VerifyParameter
				  
				  else
				   LOG_MESSAGE="DELAY_DATE_TO= $DATE_TO"
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				  fi
				  
					 
				else
					  LOG_MESSAGE="DELAY_DATE_TO is NULL or empty."
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					  VER_PARA="N"
					  VerifyParameter
				fi
		else
			LOG_MESSAGE="DELAY_FLG is not [F] or [D] or [N]."
		    echo ${LOG_MESSAGE}
		    echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			VER_PARA="N"	
			VerifyParameter
		fi
	  
else
      LOG_MESSAGE="DELAY_FLG is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
	  VerifyParameter
fi

if [ -n "${FILE_PATH}" ]; then
      LOG_MESSAGE="FILE_PATH= $FILE_PATH"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
else
      LOG_MESSAGE="FILE_PATH is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
	  VerifyParameter
fi

if [ -n "${FILE_NAME}" ]; then
      LOG_MESSAGE="FILE_NAME= $FILE_NAME"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

else
      LOG_MESSAGE="FILE_NAME is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
	  VerifyParameter
fi

if [ -n "${FILE_TYPE}" ]; then
      LOG_MESSAGE="FILE_TYPE= $FILE_TYPE"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  
		if [ $FILE_TYPE == ".dat" ]; then
				if [ -n "${FILE_CHECK}" ]; then
					  LOG_MESSAGE="FILE_CHECK= $FILE_CHECK"
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
				else
					  LOG_MESSAGE="FILE_CHECK is NULL or empty."
					  echo ${LOG_MESSAGE}
					  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
					  VER_PARA="N"
					  VerifyParameter
				fi
		elif [ $FILE_TYPE == ".txt" ] || [ $FILE_TYPE == ".csv" ]; then
			LOG_MESSAGE="File type : ${FILE_TYPE} is not use FILE_CHECK"
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		else
			LOG_MESSAGE="FILE_TYPE is not [.txt] or [.dat]. or [.csv]"
			echo ${LOG_MESSAGE}
			echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			VER_PARA="N"
			VerifyParameter			
		fi  
	  
else
      LOG_MESSAGE="FILE_TYPE is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
	  VerifyParameter
fi

if [ -n "${SCRIPT_NAME}" ]; then
      LOG_MESSAGE="SCRIPT_NAME= $SCRIPT_NAME"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE} 

	if [ ! -f ${SCRIPT_PATH}/${SCRIPT_NAME} ]; then
		LOG_MESSAGE="${SCRIPT_NAME} : the file is not found."
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		VER_PARA="N"
		VerifyParameter
	else
		LOG_MESSAGE="File= "${SCRIPT_NAME}" is found."
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}		

	fi
else
      LOG_MESSAGE="SCRIPT_NAME is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
	  VerifyParameter
fi

if [ -n "${SPLIT_FLG}" ]; then
      LOG_MESSAGE="  SPLIT_FLG= $SPLIT_FLG"
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE} 

	if [ $SPLIT_FLG == "Y" ]; then

		if [ -n "${SPLIT_ROW}" ]; then
			  LOG_MESSAGE="SPLIT_ROW= $SPLIT_ROW"
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		else
			  LOG_MESSAGE="SPLIT_ROW is NULL or empty."
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			  VER_PARA="N"
			  VerifyParameter
		fi

		if [ -n "${SPLIT_PATH}" ]; then
			  LOG_MESSAGE="SPLIT_PATH= $SPLIT_PATH"
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

		else
			  LOG_MESSAGE="SPLIT_PATH is NULL or empty."
			  echo ${LOG_MESSAGE}
			  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
			  VER_PARA="N"
			  VerifyParameter
		fi		
	else
		LOG_MESSAGE="SPLIT_FLG is ["${SPLIT_FLG}"]  do not check row and path for split."
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}	
	fi
else
      LOG_MESSAGE="SPLIT_FLG is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
      VER_PARA="N"
fi

if [ -n "${FILE_NAME}" ]; then
      LOG_MESSAGE="  FILE_NAME= $FILE_NAME"
	  echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  
	  POSITION_DATE="$((${#FILE_NAME} + 1))"
	  LOG_MESSAGE="POSITION_DATE :"${POSITION_DATE}
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

else
      LOG_MESSAGE="  Exit Program: FILE_NAME is NULL or empty."
      echo ${LOG_MESSAGE}
      echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
  
  	  MESSAGE="FILE_NAME is NULL or empty."
	  InsertError "$SCRIPT_NAME" "$MESSAGE" $RET_CD_CON_M "$DATA_SOURCE"
  
	  UpdateStatus $SCRIPT_NAME $RET_CD_CON_M
					  
	  LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
	  echo ${LOG_MESSAGE}
	  echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
	  exit $RET_CD_CON_M;
fi

##End Verify Parameter
VerifyParameter

LOG_MESSAGE="##Verify Parameter Completely."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}


############# 5. Process #############
LOG_MESSAGE="##Processing..."
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}

if [ $SUBFOLDER_FLG == "Y" ]; then



############# 5.0 Loop Sup Folder #############
## Command List Sub Folder ##
sqlplus -s @${CM_USERID} ${DATE_FROM} ${DATE_TO} ${SUBFOLDER_FORMAT} > $LST_SUB <<EOF
set echo off verify off feed off head off line 1000 pages 0
select distinct to_char(to_date('&&1','yyyymmdd')+ rownum-1,'&&3') 
from all_objects
where rownum <= to_date('&&2','yyyymmdd')-to_date('&&1','yyyymmdd')+1
order by 1;
EOF

ALL_FILE_FLG="N"
ALL_FOLDER_FLG="N"
SHORT_KEY="CONTROL_M_RESULT"
V_DATEFROM=$DATE_FROM
DATA_SOURCE=""
CURR_FILE=""
PREV_SUBFOLDER=""

for x in `cat ${LST_SUB}`
do
  	
	if [ $SUBFOLDER_FORMAT == "YYYYMMDD" ]; then
		V_DATEFROM=${x}
		V_DATETO=${x}
	else
		if [ -z "$SUBFOLDER" ]; then
			V_DATEFROM=$DATE_FROM
		else
			if [ $SUBFOLDER_FORMAT == "YYYYMM" ]; then
				V_DATEFROM="$SUBFOLDER01"	
			elif  [ $SUBFOLDER_FORMAT == "YYYY" ]; then
				V_DATEFROM="$SUBFOLDER0101"	
			fi
		fi
		V_DATETO=$DATE_TO
	fi
	
	SUBFOLDER=${x}
	JOB_RESULT_FLG="N"
	DATE_COUNT=""
	DATA_SOURCE=""
	

	FILE_PATH=$MFILE_PATH/$SUBFOLDER


	if [ ! -d "$FILE_PATH" ]; then
		LOG_MESSAGE="File Path Not found [ $FILE_PATH ]"
		JOB_RESULT_FLG="N"
		JOB_RESULT=2
		#LOG_MESSAGE="Exit Program: "${LOG_MESSAGE}
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		
		if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "F" ]; then
			LOG_MESSAGE="File Path Not found [ $FILE_PATH ]"
			JOB_RESULT_FLG="Y"
			JOB_RESULT=0
		fi
		
		UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	                  
	fi

	echo "SUBFOLDER :"$SUBFOLDER
	echo "FILE PATH :"${FILE_PATH}
	cd ${FILE_PATH}
	CheckFile
	

PREV_SUBFOLDER=$SUBFOLDER
done  

else
		##FILE_PATH=${MFILE_PATH}/${SUBFOLDER}
		FILE_PATH=${MFILE_PATH}
		
		if [ ! -d "$FILE_PATH" ]; then
		LOG_MESSAGE="File Path Not found [ $FILE_PATH ]"
		JOB_RESULT_FLG="N"
		JOB_RESULT=2
		#LOG_MESSAGE="Exit Program: "${LOG_MESSAGE}
		echo ${LOG_MESSAGE}
		echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
		
		if [ $DELAY_FLG == "N" ] && [ $GET_FILE == "F" ]; then
			LOG_MESSAGE="File Path Not found [ $FILE_PATH ]"
			JOB_RESULT_FLG="Y"
			JOB_RESULT=0
		fi
		
		UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	                  
	fi
		cd ${FILE_PATH}
		CheckFile
		
fi

LOG_MESSAGE="  Exit Program: "${LOG_MESSAGE}
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
UpdateParameter "$JOB_RESULT_FLG|$V_DATEFROM|$DATE_TO|$JOB_RESULT|$SHORT_KEY|$LOG_MESSAGE|$DATA_SOURCE|$GET_FILE"	
LOG_MESSAGE="==================== FINISH TIME "`date +%d/%m/%Y_%H:%M:%S`" ===================="
echo ${LOG_MESSAGE}
echo ${LOG_MESSAGE} >> $LOG_PATH/${LOG_FILE}
exit ${v_result}; 
