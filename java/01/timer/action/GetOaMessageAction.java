package ces.timer.action;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import ces.gdda.archives.borrowUsing.bean.ArchiveBorrow;
import ces.gdda.common.ArchiveCommon;


@SuppressWarnings("unused")
public class GetOaMessageAction {
	
	//����oaϵͳ���ݿ��jdbc
	private JdbcTemplate oaJdbcTemplate = null;
	//����ϵͳ��jdbc
	private JdbcTemplate dagJdbcTemplate = null;
	/*
	 * ��Ҫ��ȡ�ñ��
	*/
	//select * from tbl_lg_hqc_gwcld;										#���ڴ����Ĵ���
	private final String OA_HQC_GW = "tbl_lg_hqc_gwcld";					
	//select * from tbl_lg_jcsjc_swcld;										#�ͼ촦���Ĵ���
	private final String OA_JJC_ACCEPT = "tbl_lg_jcsjc_swcld";
	//	select * from tbl_lg_cwc_fwcld;										#���񴦷��Ĵ���
	private final String OA_CWC_SEND = "tbl_lg_cwc_fwcld";
	//	select * from tbl_lg_kjc_kjcfwlc;									#�Ƽ������Ĵ���
	private final String OA_KJC_SEND = "tbl_lg_kjc_kjcfwlc";
	//	select * from tbl_lg_jwc_zsbgwcld;									#���������칫�Ĵ���
	private final String OA_JWC_ZSB_SEND = "tbl_lg_jwc_zsbgwcld";
	//	select * from tbl_lg_jwc_gwcld;										#���񴦹��Ĵ���
	private final String OA_JWC_GW = "tbl_lg_jwc_gwcld";
	//	select * from tbl_xzfwnew;											#УԺ��ѧУ���ģ��£�
	private final String OA_XYB_XX_SEND_NEW1 = "tbl_xzfwnew";
	//	select * from tbl_frmBulletin;										#УԺ��ѧУ���ģ��£�
	private final String OA_XYB_XX_SEND_NEW2 = "tbl_frmBulletin";
	//	select * from tbl_zzbgd;											#УԺ��ѧУ���ģ��£�
	private final String OA_XYB_XX_ACCEPT_NEW1 = "tbl_zzbgd";
	//	select * from tbl_lg_xyb_yuewen;									#УԺ��ѧУ���ģ��£�
	private final String OA_XYB_XX_ACCEPT_NEW2 = "tbl_lg_xyb_yuewen";
	//	select * from tbl_lg_xyb_yuewen;									#УԺ��ѧУ����     
	//private final String OA_XYB_ACCEPT_OLD = "tbl_lg_xyb_yuewen";
	//	select * from tbl_lg_xyb_xnfwv1_1;									#УԺ��У�췢��
	private final String OA_XYB_XB_ACCEPT_SEND1 = "tbl_lg_xyb_xnfwv1_1";
	//	select * from tbl_lg_xyb_xnfw;										#УԺ��У�췢��
	private final String OA_XYB_XB_ACCEPT_SEND2 = "tbl_lg_xyb_xnfw";
	//	select * from tbl_xgcfw;											#ѧ��������
	private final String OA_XGC_SEND = "tbl_xgcfw";
	//	select * from tbl_lg_zzb_fwcld;										#��֯�����Ĵ���
	private final String OA_ZZB_SEND = "tbl_lg_zzb_fwcld";
	//	select * from tbl_lg_jcsjc_swcld;									#�����ƴ����Ĵ���
	private final String OA_JCSJC_ACCEPT = "tbl_lg_jcsjc_swcld";
	//	select * from tbl_lg_db_swcld;										#�������Ĵ���
	private final String OA_DB_ACCEPT = "tbl_lg_db_swcld";
	//	select * from tbl_lg_db_dwfw;										#��ί����
	private final String OA_DW_SEND = "tbl_lg_db_dwfw";
	/**
	 * oa������
	 */
	private final  String OA_FILE_TBL = "tbl_file";							//oa������
	
	private final String OA_FILE_URL = "http://210.34.213.66/servlet/ArchiveFileServlet?";
	
	//http://210.34.213.66/servlet/ArchiveFileServlet?n=����n&id=����id
	//����n�����ݿ��ļ���(c_name�ֶ�)��urlencode ����
	//����id�����ݿ��ļ�·����c_path�ֶΣ���urlencode����
	
	String currOATableName = null;//��ǰ������OA����
	String currOATableID = null;//��ǰ������OA�����id
	
	
	/**
	 * �м����¼�Ѿ�����ȡ�ļ�¼
	 */
	private final String OA_GETED = "oa_zx_geted";
	/**
	 * ��Ҫ
	 */
	private final String DAG_T_AR_XZ_FILE = "T_FILE_XZ";
	private final String DAG_DOCUMENT = "T_ARCHIVE_DOCUMENT";				//������
	private final String DAG_DOCUMET_BASE_PATH = "";
	/**
	 * ����״̬��Ĭ���ϸ�timer��û��ִ�н���
	 */
	public int thisTaskStatus = 0;
	
	/**
	 * ���sql��Sb
	 */
	StringBuilder currGetOaSb = new StringBuilder();//----------------------ר��ƴ�ӻ�ȡoa���ݵ�sql
	StringBuilder currInsertDagSb = new StringBuilder();//------------------ר��ƴ�ӽ�oa���ݲ��뵽����ϵͳ��sql
	StringBuilder rollBackSb = new StringBuilder();//-----------------------ר��ƴ�ӻ���м��ĵ�sql
	/**
	 * ��ʱ��Ҫִ�еķ���
	 * @throws InterruptedException 
	 */
	public void getOaMessage() throws InterruptedException{
		
		//System.out.println("this is a timer for getOaMessage! >  " + thisTaskStatus);
		
		
		if(thisTaskStatus==1){
			//System.out.println("���ڴ���ǰ100�����ݣ�������");
		}else{
			/** 
			 * =============================================
			 *	ÿ����һǧ��һǧ������
			 *  1���ж���һ��1000���Ƿ�ִ�н������� Ȼ�� ���1000��δ���������
			 *  2��ѭ��1000�������޸��м��
			 *  3���޸��м�����뵽����ϵͳ
			 * =============================================
			 */
			this.thisTaskStatus = 1;
			
			try {
				/**
				 * �߼�һ�����ŵ��������ѭ�������ٴ���
				 */
				//����
				String[] tables = {
						OA_HQC_GW, 				//01 "tbl_lg_hqc_gwcld";		#���ڴ����Ĵ���				
						OA_JJC_ACCEPT, 			//02 "tbl_lg_jcsjc_swcld";		#�ͼ촦���Ĵ���
						OA_CWC_SEND, 			//03 "tbl_lg_cwc_fwcld";		#���񴦷��Ĵ���
						OA_KJC_SEND, 			//04 "tbl_lg_kjc_kjcfwlc";		#�Ƽ������Ĵ���
						OA_JWC_ZSB_SEND, 		//05 "tbl_lg_jwc_zsbgwcld";		#���������칫�Ĵ���
						OA_JWC_GW, 				//06 "tbl_lg_jwc_gwcld";		#���񴦹��Ĵ���
						OA_XYB_XX_SEND_NEW1, 	//07 "tbl_xzfwnew";				#УԺ��ѧУ���ģ��£�
						OA_XYB_XX_SEND_NEW2, 	//08 "tbl_frmBulletin";			#УԺ��ѧУ���ģ��£�
						OA_XYB_XX_ACCEPT_NEW1, 	//09 "tbl_zzbgd";				#УԺ��ѧУ���ģ��£�
						OA_XYB_XX_ACCEPT_NEW2, 	//10 "tbl_lg_xyb_yuewen";		#УԺ��ѧУ���ģ��£�
						OA_XYB_XB_ACCEPT_SEND1, //11 "tbl_lg_xyb_xnfwv1_1";		#УԺ��У�췢��
						OA_XYB_XB_ACCEPT_SEND2, //12 "tbl_lg_xyb_xnfw";			#УԺ��У�췢��
						OA_XGC_SEND, 			//13 "tbl_xgcfw";				#ѧ��������
						OA_ZZB_SEND, 			//14 "tbl_lg_zzb_fwcld";		#��֯�����Ĵ���
						OA_JCSJC_ACCEPT, 		//15 "tbl_lg_jcsjc_swcld";		#�����ƴ����Ĵ���
						OA_DB_ACCEPT, 			//16 "tbl_lg_db_swcld";			#�������Ĵ���
						OA_DW_SEND, 			//17 "tbl_lg_db_dwfw";			#��ί����
				};
				//ÿ�����Ӧ���ֶ�
				String[] tblCloumns = {
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_Body,c_Date,c_Writer,c_Dept,c_Notions_KSFZR,c_Notions_BMFZE,c_Notions_XGBM,c_Notions_FGXLD,c_reason,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//1
						"c_Received,c_Urgency,c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_DUsers,c_s_ResponseBy,c_ToDate,c_OriginFileDate,c_Importance,c_Notions_JWSJ,c_Notions_JCSJC,c_Notions_XGRY",//2
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Body,c_Date,c_Writer,c_Dept,c_Notions_KZ,c_Notions_FCZ,c_Notions_CZ,c_Notions_ZLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//3
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notions_BMLD,c_Notion_HG,c_Notions_FGXLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//4
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notions_KC,c_Notions_XGBMHG,c_Notions_ZR,c_Notions_FGXLD,c_Notions_NGRXGJD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//5
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notions_KC,c_Notions_XGKSHG,c_Notions_FGCZSG,c_Notions_XGBMHG,c_Notions_CLD,c_Notions_FGXLD,c_Notions_NGRXGJD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//6
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notion_YGR,c_Notions_BMLD,c_Notions_XGBM,c_Notions_FGXLDHQ,c_Notions_WMKYS,c_Notions_XBZR,c_Notions_FGXLD,c_Notions_XD,c_XDR,c_XDSJ,c_Notions_WMK,c_Date_1,c_Title,c_SendUnit,c_Phone,c_PrnCopies,c_KeyWords,c_ToInUnit,c_CcInUnit",//7
						"c_s_DocNo,c_Title,c_Received,c_OriginFileDate,c_Importance,c_Urgency,c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_TypeDJ,c_Type,c_DUsers,c_Name_1,c_Name_2,c_s_ResponseBy,c_ToDate,c_Notions_YBZR,c_Notions_LDBM,c_Notions_WMKFS,c_Notions_XLDPB,c_Notions_XGLD,c_Notions_ZNBM,c_Notions_CYBM,c_Notions_XBQL",//8
						"c_Title,c_SN_1,c_Body",//9
						"c_SN,c_Title,c_Received,c_OriginFileDate,c_Importance,c_Urgency,c_FileNo,c_Page,c_TypeNo,c_DUsers,c_s_ResponseBy,c_ToDate,c_Notions_YBZR,c_Notions_WMKFS,c_Notions_XLDPB,c_Notions_XGLD,c_Notions_ZNBM,c_Notions_CYBM,c_Notions_XBQL",//10
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Notions_BMLD,c_Notion_HG,c_Notions_FGXLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//11
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Notions_BMLD,c_Notion_HG,c_Notions_FGXLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//12
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notions_YGR,c_Notions_HG,c_Notions_XGBM,c_Notions_FGXLDHQ,c_Notions_FGXLD,c_Notions_XD,c_XDR,c_XDSJ,c_Notions_WMK,c_Date_1,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//13
						"c_s_DocNo,c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Notion_KZ,c_Notions_BMFZ,c_Notions_BMLD,c_Notions_XGBM,c_Notions_ZZB,c_Notions_XLDYJ,c_Notions_BHDY,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit",//14
						"c_Received,c_Urgency,c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_DUsers,c_s_ResponseBy,c_ToDate,c_OriginFileDate,c_Importance,c_Notions_JWSJ,c_Notions_JCSJC,c_Notions_XGRY",//15
						"c_Received,c_Urgency,c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_DUsers,c_s_ResponseBy,c_ToDate,c_Notions_YBZR,c_Notions_FGXLD,c_Notions_XGRY,c_Notions_QTCY,c_Notions_DBQK",//16
						"c_Type,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date,c_Writer,c_Dept,c_Notions_BMLD,c_Notions_XGBM,c_Notions_FGXLDHQ,c_Notions_BGS,c_Notions_DBZR,c_Notions_FGXLD,c_Notions_XLD,c_Notions_XD,c_XDR,c_XDSJ,c_Notions_WMK,c_Date_1,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit" //17
				};
				/**
				 * OA����Ҫ���ֶ� һһ��Ӧ
				 */
				String[]  tblCloumns_need = {
						"c_s_DocNo,c_Body,c_Title,c_KeyWords",//----------------------------------------------01	tbl_lg_hqc_gwcld							
						"c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_s_ResponseBy",//------------------------------02	tbl_lg_jcsjc_swcld							
						"c_s_DocNo,c_Body,c_Date,c_Title,c_KeyWords",//---------------------------------------03	tbl_lg_cwc_fwcld												
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------04	tbl_lg_kjc_kjcfwlc													
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------05	tbl_lg_jwc_zsbgwcld												
						"c_s_DocNo,c_Title,c_KeyWords",//-----------------------------------------------------06														
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------07
						"c_s_DocNo,c_Title,c_OriginFileDate,c_BGLimited,c_s_ResponseBy",//--------------------08							
						"c_Title,c_SN_1,c_Body",//------------------------------------------------------------09	
						"c_Title,c_OriginFileDate,c_FileNo,c_s_ResponseBy",//---------------------------------10										
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------11													
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------12													
						"c_s_DocNo,c_Date,c_Title,c_KeyWords",//----------------------------------------------13													
						"c_s_DocNo,c_Date,c_KeyWords",//------------------------------------------------------14														
						"c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_s_ResponseBy,c_OriginFileDate",//-------------15					
						"c_FileNo,c_Page,c_TypeNo,c_BGLimited,c_s_ResponseBy",//------------------------------16									
						"c_Date,c_KeyWords"//-----------------------------------------------------------------17	
				};
				
				String[] insertCloumns = {
						"FILE_NO,REMARK,TITLE_PROPER,TITLE",//------------------------------------------------01	tbl_lg_hqc_gwcld															
						"FILE_NO,AMOUNT_OF_PAGES,SERIES_CODE,RETENTION_PERIOD,AUTHOR",//----------------------02	tbl_lg_jcsjc_swcld	           				
						"FILE_NO,REMARK,DATE_OF_CREATION,TITLE_PROPER,TITLE",//-------------------------------03	tbl_lg_cwc_fwcld	                                 	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------04	tbl_lg_kjc_kjcfwlc		                                         	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------05	tbl_lg_jwc_zsbgwcld			                                        	
						"FILE_NO,TITLE_PROPER,TITLE",//-------------------------------------------------------06                                                          	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------07                                         	
						"FILE_NO,TITLE_PROPER,DATE_OF_CREATION,RETENTION_PERIOD,AUTHOR",//--------------------08                      	
						"TITLE_PROPER,FILE_NO,REMARK",//------------------------------------------------------09                                                         	
						"TITLE_PROPER,DATE_OF_CREATION,FILE_NO,AUTHOR",//-------------------------------------10                                        	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------11                                         	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------12                                         	
						"FILE_NO,DATE_OF_CREATION,TITLE_PROPER,TITLE",//--------------------------------------13                                         	
						"FILE_NO,DATE_OF_CREATION,TITLE", //--------------------------------------------------14                                                    	
						"FILE_NO,AMOUNT_OF_PAGES,SERIES_CODE,RETENTION_PERIOD,AUTHOR,DATE_OF_CREATION",//-----15       	
						"FILE_NO,AMOUNT_OF_PAGES,SERIES_CODE,RETENTION_PERIOD,AUTHOR",//----------------------16                        	
						"DATE_OF_CREATION,TITLE"//------------------------------------------------------------17
				};
				
				for(int tblIn = 0;tblIn<tables.length;tblIn++){
					String currTableName = tables[tblIn];														//��ǰ����ı���
					currOATableName = currTableName;															//��ǰ�������					
					currGetOaSb.setLength(0);
					currGetOaSb.append(" SELECT T.c_id,");
					currGetOaSb.append(tblCloumns_need[tblIn]);
					currGetOaSb.append(" from ");
					currGetOaSb.append(tables[tblIn]).append(" T ");
					currGetOaSb.append(" WHERE NOT EXISTS ");
					currGetOaSb.append(" (SELECT  1 FROM  ").append(OA_GETED).append(" P WHERE  T.c_id = P.oa_table_id) ");
					currGetOaSb.append(" LIMIT 0,100 ");
					//System.out.println(currGetOaSb.toString());
					List<?> currOaList = oaJdbcTemplate.queryForList(currGetOaSb.toString());
					if(currOaList.size()>0){
						for(int i = 0 ; i < currOaList.size() ; i++) {
							/**
							 * @step1  �жϵ�ǰ��¼�Ƿ��Ѿ����뵽�м����
							 */
							Object currIntId = ((Map)currOaList.get(i)).get("c_id");
							currOATableID = currIntId.toString();
							currGetOaSb.setLength(0);
							currGetOaSb.append("select id from ").append(OA_GETED).append(" t where t.oa_table = ").append("'").append(currTableName).append("' and t.oa_table_id = '").append(currIntId).append("'");
							if(oaJdbcTemplate.queryForList(currGetOaSb.toString()).size()!=0){
								/*
								 * @step2 �޸��м�� ״̬ 3 ������
								 */
								//INSERT INTO oa_zx_geted (oa_table,oa_table_id,status) values('TABL1','3999','1');
								currGetOaSb.setLength(0);
								currGetOaSb.append("update ").append(OA_GETED).append(" set status = '1' where oa_table = '").append(currTableName).append("' and oa_table_id = '").append(currIntId).append("'");
								oaJdbcTemplate.update(currGetOaSb.toString());
							}else{
								/*
								 * @step2 ���뵽�м�� ״̬ 3 ������
								 */
								//INSERT INTO oa_zx_geted (oa_table,oa_table_id,status) values('TABL1','3999','1');
								currGetOaSb.setLength(0);
								currGetOaSb.append("INSERT INTO ").append(OA_GETED).append("(");
								currGetOaSb.append("oa_table,oa_table_id,status");
								currGetOaSb.append(") VALUES (");
								currGetOaSb.append("'").append(currTableName).append("',");
								currGetOaSb.append("'").append(currIntId).append("',");
								currGetOaSb.append("'3')");
								oaJdbcTemplate.update(currGetOaSb.toString());
							}
							/*
							 * @step3 ����Ŀ������ȡ��������ϵͳ������������
							 * ARCHIVE_NO, AUTHOR,FILE_NO, TITLE_PROPER,DATE_OF_CREATION,AMOUNT_OF_PAGES,COMBIN_ASSOCIATE_STATUS,ASSOCIATE_FLAG,STATUS
							 */
							//TODO jingjianqian 
							currInsertDagSb.setLength(0);
							currInsertDagSb.append("SELECT MAX(ID) AS CUR_ID FROM ").append(DAG_T_AR_XZ_FILE);
							
							Object currIntIdObj  = ((Map)dagJdbcTemplate.queryForList(currInsertDagSb.toString()).get(0)).get("CUR_ID");
							int currInt = Integer.parseInt(currIntIdObj.toString());
							int currColLen = insertCloumns[tblIn].split(",").length;				//��ǰ���Զ����ֶθ���
							String[] currInsertColStrArr = insertCloumns[tblIn].split(",");			//�����ֶ�
							String[] curColValStrArr = tblCloumns_need[tblIn].split(",");			//�ֶ�ֵ��Ӧ���ֶ�key
							currInsertDagSb.setLength(0);
							currInsertDagSb.append("INSERT INTO ").append(DAG_T_AR_XZ_FILE);
							currInsertDagSb.append("(id,CATALOGUE_NAME,STATUS,ARCHIVE_TYPE_ID,fonds_code,FILING_DEPT,");
							for(int curColIn = 0;curColIn<currColLen;curColIn++){
								currInsertDagSb.append(currInsertColStrArr[curColIn]);
								if(curColIn!=currColLen-1){
									currInsertDagSb.append(",");
								}
							}
							currInsertDagSb.append(")VALUES(");
							/**
							 * �̶��ֶ�
							 */
							currInsertDagSb.append(currInt+1).append(",");					//id
							currInsertDagSb.append("'").append("OAϵͳ").append("',");		//CREATE_USER
							currInsertDagSb.append("'").append("01").append("',");			//STATUS
							currInsertDagSb.append("").append("10832").append(",");			//ARCHIVE_TYPE_ID
							currInsertDagSb.append("'210',");								//fonds_code
							currInsertDagSb.append("'1063',");								//FILING_DEPT
							/**
							 * �Զ���ƥ���ֶ�
							 */
							for(int curColIn = 0;curColIn<currColLen;curColIn++){
								currInsertDagSb.append("'");
								//System.out.println(currOaList);
								//System.out.println(((Map)currOaList.get(i)));
								//System.out.println(curColValStrArr[curColIn]);
								currInsertDagSb.append(((Map)currOaList.get(i)).get(curColValStrArr[curColIn])==null?"":((Map)currOaList.get(i)).get(curColValStrArr[curColIn]));
								currInsertDagSb.append("'");
								if(curColIn!=currColLen-1){
									currInsertDagSb.append(",");
								}
							}
							currInsertDagSb.append(")");
							dagJdbcTemplate.execute(currInsertDagSb.toString());
							/*
							 * @step4 ����Ӧ�ĸ������������������Ҳ��������¼
							 */
							//TODO jingjianqian 
							currGetOaSb.setLength(0);
							currGetOaSb.append("select c_name,c_path from ").append(OA_FILE_TBL).append(" where c_mainid = '").append(currIntId).append("'");
							List  oa_file_map = oaJdbcTemplate.queryForList(currGetOaSb.toString());
							//System.out.println(oa_file_map);
							if(oa_file_map.size()>0){
								for(int currFileInt = 0;currFileInt<oa_file_map.size();currFileInt++){
									Map currFileMap = (Map) oa_file_map.get(currFileInt);
									StringBuilder currFileURL = new StringBuilder();
									Object _n	 	  = currFileMap.get("c_name")==null?"":currFileMap.get("c_name");
									Object _path 	  = currFileMap.get("c_path")==null?"":currFileMap.get("c_path");
									String _n_code	  = java.net.URLEncoder.encode(_n.toString(),"UTF-8");
									String _path_code = java.net.URLEncoder.encode(_path.toString(),"UTF-8");
									currFileURL.append(OA_FILE_URL).append("n=").append(_n_code).append("&id=").append(_path_code);
									//System.out.println(currFileURL.toString());
									//this.downloadNet(OA_FILE_URL,_n.toString());
									/**
									 * @step5 ���������������������Ҳ��븽�����¼��������
									 * ������ DAG_DOCUMENT = "T_ARCHIVE_DOCUMENT";
									 */
								
									StringBuilder sb = new StringBuilder();												 //�ļ����ػ���·��
							        Calendar date = Calendar.getInstance();												 //P@ssw0rd
							        String year = String.valueOf(date.get(Calendar.YEAR));								 					
							        String month = String.valueOf(date.get(Calendar.MONTH));		
							        String rootPath = ArchiveCommon.getDocumentPath();
							        sb.append(rootPath).append("/").append("OA").append("/").append(year).append("/").append(month).append("/");		 		//���ش�ŵĻ���·��		
									StringBuilder xdljPath = new StringBuilder();										 //�����ݿ����·��
									xdljPath.append("/OA/").append(year + "/" + month).append("/").append(_n.toString());//���·��
									currInsertDagSb.setLength(0);
									currInsertDagSb.append("SELECT MAX(ID) AS CUR_ID FROM ").append(DAG_DOCUMENT.toString());
									Object currDocIntIdObj  = ((Map)dagJdbcTemplate.queryForList(currInsertDagSb.toString()).get(0)).get("CUR_ID");
									//System.out.println(sb.toString());
									new GetOaMessageAction().downloadNet(currFileURL.toString(),sb.toString(),_n.toString());
									
									int currDocInt = Integer.parseInt(currDocIntIdObj.toString());
									currInsertDagSb.setLength(0);
									
									String _file_name = _n.toString();
									String _file_title_proper = _file_name.substring(0,_file_name.lastIndexOf("."));
									String _file_format = _file_name.substring(_file_name.lastIndexOf(".")+1);
									currInsertDagSb.append("insert into ").append(DAG_DOCUMENT.toString()).append("(");
									currInsertDagSb.append("id,"
											+ "archive_type_id,"
											+ "owner_id,"
											+ "status,"
											+ "fonds_code,"
											+ "filing_dept,"
											+ "title_proper,"
											+ "version,"
											+ "is_delete,"
											+ "file_name,"
											+ "path,"
											+ "file_format,"
											+ "file_size,"
											+ "type,"
											+ "key_word,"
											+ "file_browse_path) values(");
									currInsertDagSb.append("'").append(currDocInt+1).append("',");//---------------------id
									currInsertDagSb.append("'").append(10832).append("',");	//---------------------------��������ID
									currInsertDagSb.append("'").append(currInt+1).append("',");	//-----------------------������ĿID
									currInsertDagSb.append("'").append(01).append("',");	//---------------------------״̬
									currInsertDagSb.append("'").append(210).append("',");	//---------------------------����ȫ��
									currInsertDagSb.append("'").append(1063).append("',");	//---------------------------��������
									currInsertDagSb.append("'").append(_file_title_proper).append("',");//---------------��������
									currInsertDagSb.append("'").append(1).append("',");//--------------------------------�屾
									currInsertDagSb.append("'").append(0).append("',");//--------------------------------ɾ��״̬
									currInsertDagSb.append("'").append(_file_name).append("',");//-----------------------�ļ�����
									currInsertDagSb.append("'").append(xdljPath).append("',");//-------------------------�ļ�·��
									currInsertDagSb.append("'").append(_file_format).append("',");//---------------------�ļ���׺
									currInsertDagSb.append("'").append(0).append("',");//--------------------------------��С
									currInsertDagSb.append("'").append(2).append("',");//--------------------------------��������
									currInsertDagSb.append("'").append("OA").append("',");//-----------------------------��������
									currInsertDagSb.append("'").append(xdljPath).append("')");//-------------------------���·��
									dagJdbcTemplate.execute(currInsertDagSb.toString());
									
									/**
									 * @step6 ����û�б��������  currIntId+1
									 */
									currInsertDagSb.setLength(0);
									currInsertDagSb.append("SELECT TITLE_PROPER  FROM ").append(DAG_T_AR_XZ_FILE).append(" t where t.id = '").append(currInt+1).append("'");
									List _title_list = dagJdbcTemplate.queryForList(currInsertDagSb.toString());
									if(_title_list.size()>0){
										for(int currTitleLIndex = 0;currTitleLIndex<_title_list.size();currTitleLIndex++){
											Map currTitleLMap = (Map) _title_list.get(currTitleLIndex);
											Object _titles = currTitleLMap.get("TITLE_PROPER")==null?"":currTitleLMap.get("TITLE_PROPER");
											String _titStr = _titles.toString();
											if(_titStr.equals("")){
												//System.out.println(_file_title_proper);
												currInsertDagSb.setLength(0);
												currInsertDagSb.append("update ").append(DAG_T_AR_XZ_FILE).append(" set TITLE_PROPER = '").append(_file_title_proper).append("' where id = '").append(currInt+1).append("'");
												dagJdbcTemplate.execute(currInsertDagSb.toString());
											}
										}
									}
								}
							}
							/*
							 * @step5 ���м��״̬�޸�Ϊ�Ѿ�����״̬ 2
							 */
							currGetOaSb.setLength(0);
							currGetOaSb.append("update ").append(OA_GETED).append(" set status = '2' where oa_table = '").append(currTableName).append("' and oa_table_id = '").append(currIntId).append("'");
							oaJdbcTemplate.update(currGetOaSb.toString());
						}
					}else{
						//System.out.println(currOaList.size());
					}
				}
			} catch (Exception e) {
				/**
				 * ������
				 */
				if(currOATableName!=null&&currOATableID!=null){
					
					
					currGetOaSb.setLength(0);
					currGetOaSb.append("update ").append(OA_GETED).append(" set status = '1' where oa_table = '").append(currOATableName).append("' and oa_table_id = '").append(currOATableID).append("'");
					oaJdbcTemplate.update(currGetOaSb.toString());
					
					
					//rollBackSb.setLength(0);
					//rollBackSb.append("DELETE FROM ").append(OA_GETED).append("  WHERE OA_TABLE = '").append(currOATableName).append("' and OA_TABLE_ID = '").append(currOATableID).append("'");
					//System.out.println(rollBackSb.toString());
					try{
						oaJdbcTemplate.execute(currGetOaSb.toString());
					}catch(Exception error){
						error.printStackTrace();
					}
				}
				e.printStackTrace();
			}finally{
				this.thisTaskStatus = 0;
			}
		}
	}
	/**
	 * main������
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String args[]) throws MalformedURLException{
		 String cesRoot = System.getProperty("cesRoot");
		 System.out.println(cesRoot);
		/*StringBuilder sb = new StringBuilder();									//�ļ����ػ���·��
        Calendar date = Calendar.getInstance();									
        String year = String.valueOf(date.get(Calendar.YEAR));					
        String month = String.valueOf(date.get(Calendar.MONTH));				
        
		sb.append("E:/gdda_files/affix/OA/").append(year + "/" + month).append("/");		
		
		//System.out.println(sb.toString());
		StringBuilder xdljPath = new StringBuilder();							//�����ݿ����·��
		xdljPath.append("/OA/").append(year + "/" + month).append("/").append("201805301616510.doc".toString());
		System.out.println(xdljPath.append("").toString());
		
		new GetOaMessageAction().downloadNet("http://210.34.213.66/servlet/ArchiveFileServlet?n=201805301616510.doc&"
				+ "id=%2F2018%2F06%2F5D808E798695E004482582A200305F92%2F201805301616510.doc",sb.toString(),"test.doc");*/
	}
	/**
	 * �����ļ�����
	 * @param urlPath
	 * @param basePath
	 * @param fileName
	 * @throws MalformedURLException
	 */
	public void downloadNet(String urlPath,String basePath,String fileName) throws MalformedURLException {
        // ���������ļ�
        int bytesum = 0;
        int byteread = 0;

        URL url = new URL(urlPath);
       
        StringBuilder filePath = new StringBuilder();
       // System.out.println(fileName);
        filePath.append(basePath);
        File file = new File(filePath.toString());
        if(!file.exists()){
        	file.mkdirs();
        }
        filePath.append(fileName);
        try {
            URLConnection conn = (URLConnection) url.openConnection();
            InputStream inStream = conn.getInputStream();
            FileOutputStream fs = new FileOutputStream(filePath.toString());
            byte[] buffer = new byte[1204];
            int length;
            while ((byteread = inStream.read(buffer)) != -1) {
                bytesum += byteread;
                fs.write(buffer, 0, byteread);
            }
            fs.close();
            inStream.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }finally{
        	/*try {
        		inStream.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}*/
        }
    }
	/**
	 * setter and getter
	 * @return
	 */
	public JdbcTemplate getOaJdbcTemplate() {
		return oaJdbcTemplate;
	}

	public void setOaJdbcTemplate(JdbcTemplate oaJdbcTemplate) {
		this.oaJdbcTemplate = oaJdbcTemplate;
	}

	public JdbcTemplate getDagJdbcTemplate() {
		return dagJdbcTemplate;
	}

	public void setDagJdbcTemplate(JdbcTemplate dagJdbcTemplate) {
		this.dagJdbcTemplate = dagJdbcTemplate;
	}
	
}
