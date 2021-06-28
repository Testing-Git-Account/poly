import pandas as pd


class ModelNProcessor:
    def __init__(self, ModelN_FilePath):
        print("Reading Model-N File")
        self.df_ModelN_Data= pd.read_excel(ModelN_FilePath,sheet_name='Data')

    def processor(self, partner_file_id, submitter_name):
        # Filter rows on bases of Partner File ID and Submitter Name
        df_Model_Filter_Data = self.df_ModelN_Data[(self.df_ModelN_Data['DATA_FILE_SID'] == int(partner_file_id)) &
                                                   (self.df_ModelN_Data['SUBMITTER_NAME'] == submitter_name)]
        # Return Specific columns
        df_Model_Filter_Data = pd.DataFrame(df_Model_Filter_Data,
                                            columns=['TOTAL_QUANTITY', 'OBIEE_QUANTITY', 'RESUBMIT_QUANTITY',
                                                     'REJECT_QUANTITY', 'UNACCOUNTED_QUANTITY'])
        return df_Model_Filter_Data

#if __name__=='__main__':
    #class_obj= ModelNProcessor("Input Files\Model N.xlsx")


