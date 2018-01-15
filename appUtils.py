import Utils

cm = Utils.CIXSManager()
cm.does_CIXS_Master_Exist()
cm.create_CIXS_Tables()
cm.CIXS_Update_Table_Data()
cm.CIXS_Master_Add_Field("zScore_30", "YES")
cm.CIXS_Table_Refresh_Fields()