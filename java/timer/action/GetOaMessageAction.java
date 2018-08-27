package ces.timer.action;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

@SuppressWarnings("unused")
public class GetOaMessageAction {
	
	
	private JdbcTemplate oaJdbcTemplate = null;
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
	 * �м����¼�Ѿ�����ȡ�ļ�¼
	 */
	private final String OA_GETED = "oa_zx_geted";
	/**
	 * ��Ҫ
	 */
	private final String DAG_T_AR_XZ_FILE = "T_FILE_XZ";
	/**
	 * ����״̬��Ĭ���ϸ�timer��û��ִ�н���
	 */
	public int thisTaskStatus = 0;
	/**
	 * ��ʱ��Ҫִ�еķ���
	 * @throws InterruptedException 
	 */
	public void getOaMessage() throws InterruptedException{
		
		System.out.println("this is a timer for getOaMessage! >  " + thisTaskStatus);
		if(thisTaskStatus==1){
			System.out.println("���ڴ���ǰ100�����ݣ�������");
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
				for(int tblIn = 0;tblIn<tables.length;tblIn++){
					String currTableName = tables[tblIn];//��ǰ����ı���
					StringBuilder currGetOaSb = new StringBuilder();
					currGetOaSb.append(" SELECT T.c_id,");
					currGetOaSb.append(tblCloumns[tblIn]);
					currGetOaSb.append(" from ");
					currGetOaSb.append(tables[tblIn]).append(" T ");
					currGetOaSb.append(" WHERE NOT EXISTS ");
					currGetOaSb.append(" (SELECT  1 FROM  ").append(OA_GETED).append(" P WHERE  T.c_id = P.oa_table_id) ");
					currGetOaSb.append(" LIMIT 0,100 ");
					System.out.println(currGetOaSb.toString());
					List<?> currOaList = oaJdbcTemplate.queryForList(currGetOaSb.toString());
					if(currOaList.size()>0){
						for(int i = 0 ; i < currOaList.size() ; i++) {
							/**
							 * @step1  �жϵ�ǰ��¼�Ƿ��Ѿ����뵽�м����
							 */
							Object currIntId = ((Map)currOaList.get(i)).get("c_id");
							currGetOaSb.setLength(0);
							currGetOaSb.append("select id from ").append(OA_GETED).append(" t where t.oa_table = ").append("'").append(currTableName).append("' and t.oa_table_id = '").append(currIntId).append("'");
							if(oaJdbcTemplate.queryForList(currGetOaSb.toString()).size()!=0){
								/*
								 * @step2 �޸��м�� ״̬ 3 ������
								 */
								//INSERT INTO oa_zx_geted (oa_table,oa_table_id,status) values('TABL1','3999','1');
								currGetOaSb.setLength(0);
								currGetOaSb.append("update ").append(OA_GETED).append(" set status = '3' where oa_table = '").append(currTableName).append("' and oa_table_id = '").append(currIntId).append("'");
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
							 */
							//TODO jingjianqian 
							/*
							 * @step4 ����Ӧ�ĸ������������������Ҳ��������¼
							 */
							//TODO jingjianqian 
							/*
							 * @step5 ���м��״̬�޸�Ϊ�Ѿ�����״̬ 2
							 */
							currGetOaSb.setLength(0);
							currGetOaSb.append("update ").append(OA_GETED).append(" set status = '2' where oa_table = '").append(currTableName).append("' and oa_table_id = '").append(currIntId).append("'");
							oaJdbcTemplate.update(currGetOaSb.toString());
						}
					}else{
						System.out.println(currOaList.size());
					}
				}
				/**
				 * NO1  #���������칫�Ĵ���
				 * =============================================
				 *  c_s_DocNo,c_Type  ,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date ,c_Writer,c_Dept  ,c_Notions_BMLD,c_Notion_HG,c_Notions_FGXLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit
					�����ֺ�	 ,�������� ,ǩ����  ,ǩ������  ,�ܼ�                ,��������     ,��������       ,����          ,�������� ,�������,�����     ,��崦��,�������쵼�˸�  ,��ز��Ż��,�ֹ�У�쵼ǩ��   ,����       ,���ĵ�λ     ,�绰       ,�����        ,��ӡ����       ,���͵�λ     ,���͵�λ
				 * =============================================
				 */
				//��Ҫ���ֶ�
				/*StringBuilder OA_CWC_SEND_CL_SB = new StringBuilder();
				OA_CWC_SEND_CL_SB.append("c_id,c_s_DocNo,c_Type  ,c_QFMan,c_QFDate,c_Importance,c_overtime,c_closedata,c_Urgency,c_reason,c_Date ,c_Writer,c_Dept ,");
				OA_CWC_SEND_CL_SB.append("c_Notions_BMLD,c_Notion_HG,c_Notions_FGXLD,c_Title,c_SendUnit,c_Phone,c_KeyWords,c_PrnCopies,c_ToInUnit,c_CcInUnit");
				//ƴ��SQL
				StringBuilder get_OA_CWC_SEND_SB_SQL = new StringBuilder();
				
				get_OA_CWC_SEND_SB_SQL.append(" select ");
				get_OA_CWC_SEND_SB_SQL.append(OA_CWC_SEND_CL_SB.toString());
				get_OA_CWC_SEND_SB_SQL.append(" from ");
				get_OA_CWC_SEND_SB_SQL.append(OA_KJC_SEND).append(" t ");
				get_OA_CWC_SEND_SB_SQL.append(" where not EXISTS ");
				get_OA_CWC_SEND_SB_SQL.append(" (select 1 from ").append(OA_GETED).append(" p where t.c_id = p.oa_table_id) ");
				get_OA_CWC_SEND_SB_SQL.append(" limit 0,100 ");
				
				@SuppressWarnings("rawtypes")
				List tempCWCList = oaJdbcTemplate.queryForList(get_OA_CWC_SEND_SB_SQL.toString());
				if(tempCWCList.size()>0){
					for(int i = 0 ; i < tempCWCList.size() ; i++) {
						  Map tempMap =  (Map) tempCWCList.get(i);
						System.out.println(tempMap.get("c_id"));
						}
				}*/
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				this.thisTaskStatus = 0;
			}
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
}
