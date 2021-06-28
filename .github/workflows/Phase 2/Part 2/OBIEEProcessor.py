import pandas as pd
import numpy as np


class OBIEEProcessor:
    def __init__(self, OBIEE_FilePath):
        print("Reading OBIEE File")
        self.df_OBIEE_Data = pd.read_csv(OBIEE_FilePath)
    
    def processor(self, submitterName, fiscalQtr):
        columns = ['Submitter Name', 'Submitter Oracle ID', 'Partner Reported File Name', 'FYQTR',
                   'Global POS Partner Level', 'Submitter Theater - Site', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                   '10', '11', '12', '13', 'Sales Qty']

        week_list=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

        # Extract Submitter Data from OBIEE File
        df_partnerData = self.df_OBIEE_Data[(self.df_OBIEE_Data['Submitter Name'] == submitterName) & (self.df_OBIEE_Data['FYQTR'] == fiscalQtr)]

        if df_partnerData.shape[0] == 0:
            raise Exception(f"Submitter Name - {submitterName} not found in OBIEE File.")

        # Filter out required columns from OBIEE Submitter Data
        dataframe = pd.DataFrame(df_partnerData,
                         columns=['Week No Of Fiscal Qtr','Submitter Name', 'Submitter Oracle ID',
                                  'Partner Reported File Name', 'FYQTR', 'Global POS Partner Level',
                                  'Submitter Theater - Site', 'Sales Qty'])

        dataframe1 = dataframe.groupby(['Partner Reported File Name']).agg(
            {
                'Submitter Name': 'first',
                'Submitter Oracle ID': 'first',
                'FYQTR': 'first',
                'Global POS Partner Level': 'first',
                'Submitter Theater - Site' :'first',
                'Sales Qty':sum,
            }
        )

        # Resetting index to numeric values
        dataframe1 = dataframe1.reset_index()

        # Insert 13 weeks in dataframe
        for week in week_list:
            dataframe1.insert(len(dataframe1.columns) - 1, str(week), value='')
          
        for row_index, row in enumerate(dataframe1.iterrows()):
            df_file = dataframe[dataframe["Partner Reported File Name"] == row[1]["Partner Reported File Name"]]
            df_file = df_file.groupby(['Week No Of Fiscal Qtr']).agg(
                {
                    'Sales Qty':sum,
                }
            )    
            df_file = df_file.reset_index()
            for inner_row in df_file.iterrows():
                dataframe1.at[row_index, str(inner_row[1]["Week No Of Fiscal Qtr"])] = str(inner_row[1]["Sales Qty"])
                # dataframe1.set_value(row_index, str(inner_row[1]["Week No Of Fiscal Qtr"]), str(inner_row[1]["Sales Qty"]))
            
        dataframe2 =  dataframe1.groupby(['Submitter Name']).agg(
            {
                'Submitter Name': 'first',
                '1':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '2':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '3':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '4':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '5':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '6':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '7':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '8':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '9':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '10':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '11':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '12':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                '13':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
                'Sales Qty':lambda x: int(pd.to_numeric(x, errors='coerce').sum()),
            }
        )

        #dataframe2.reset_index()
        dataframe2.at[submitterName, 'Submitter Name'] = dataframe2['Submitter Name'][0] + ' Total'
        # dataframe2.set_value(submitterName,'Submitter Name', dataframe2['Submitter Name'][0] + ' Total' )
        frames = [dataframe1, dataframe2]
        dataframe1 = pd.concat(frames, sort=False)
        dataframe1 = dataframe1.replace(np.nan, '', regex=True)
        #print(dataframe)
        dataframe1 = dataframe1[columns]
        # dataframe1.to_csv(submitterName + '.csv', index=False)
        return dataframe1
    

if __name__ == '__main__':
    class_obj = OBIEEProcessor(r"Input Files\OBIEE POS.xlsx")
    class_obj.processor("EXERTIS UK [29536]", "FY21Q2")

