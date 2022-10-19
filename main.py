from zipfile import ZipFile
import wget
import os
import pandas as pd
import chardet
import pygsheets
import sys
sys.modules.keys()
#---Global info---
url = 'https://232app.azurewebsites.net/data/BIS232Data.zip'
local_file_path = './BIS232Data.zip'
cert_file = "certs.json"
sheet_name = "tariffs"
temp_file_path = "./temp/"    
#-----------------
class GoogleSheet:
    def __init__(self, tariff_url, certs_file, sheet_name, dir):
        self._tariff_url = tariff_url
        self._certs_file = certs_file
        self._sheet_name = sheet_name
        self._working_dir = dir
        self._client = None
        self._client = self.init_client()
        self._zip_file_location = None
        self._tariff_data = None
        self._ERId = None
        self._ERId_old = None
        self._tariff_data_new = None
    def init_client(self):
        try:
            client = pygsheets.authorize(service_file=f"{self._working_dir}\\{self._certs_file}")
            print(client)
        except Exception as e:
            print(f"{self._working_dir}\\{self._certs_file}")
            print(e)
        print(client)
        return client

    def download_tariff_zip(self, local_file_path):
        # Make http request for remote file data
        print(url)
        print(local_file_path)
        if not os.path.exists(local_file_path):
            try:
                self._zip_file_location = wget.download(url, local_file_path)
            except Exception as e:
                print(e)

    def extract_zip(self):
        print("extracting zip")
        if not os.path.exists(temp_file_path):
            with ZipFile(local_file_path, 'r') as zipObj:
            # Extract all the contents of zip file in different directory
                zipObj.extractall(temp_file_path)
                print('File is unzipped in temp folder') 

        if os.path.exists(temp_file_path):
            file_list = os.listdir(temp_file_path)

            for file in file_list:
                file_path = temp_file_path + file
                if (file == 'ExclusionRequests.txt'):
                    print("Already_exists2")
                elif(file == 'ExclusionRequests.csv'):
                    print("Already_exists")
                else:
                    os.remove(file_path)

        with open(temp_file_path + 'ExclusionRequests.txt', 'rb') as file:
            print(chardet.detect(file.read()))
            print("char")

        df = pd.read_csv(temp_file_path + 'ExclusionRequests.txt', header= 0, encoding="UTF-16",on_bad_lines='skip', low_memory= False)
        columns_to_keep = ["ERId","Company","Product","PublishDate","Form_Number",
                            "Form_ExpirationDate","Product_From_JSON","HTSUSCode_From_JSON", 
                            "MetalClass","RequestingOrg_OrgLegalName", "RequestingOrg_HeadquartersCountry",
                            "RequestingImporter_OrgLegalName","RequestingImporter_HeadquartersCountry", 
                            "RequestingParent_OrgLegalName", "RequestingParent_HeadquartersCountry", 
                            "RequestingAuthRep_CountryLocation",  "ExclusionRequesterActivity","ExclusionExplanation_PercentageNotAvailable",
                            "TotalRequestedAnnualExclusionQuantity", "ExclusionExplanation_AvgAnnualConsumption",
                            "ExclusionExplanation_Explanation", 
                            "NonUSProducer_BehalfOf","NonUSProducer_ProducerName","NonUSProducer_HeadquartersCountry",
                            "SubmissionCertification_CompanyName","Created","PublicStatus"]

        self._tariff_data  = df[columns_to_keep].copy()
        self._tariff_data  = self._tariff_data.sort_values("ERId", ascending=False)
        self._ERId =  self.int_list_to_string(df['ERId'].tolist())
    def data_frame_to_excel(self, df):
        df.to_excel('output.xlsx')

    def upload_to_sheets(self, df):
        try:
            sheet = self._client.open(self._sheet_name)
        except Exception as e:
            print("Could not access sheets, if name is correct check that you shared sheet with api email ")
            raise e

        diff = self.compare_id(self.ERId, self._ERId_old)
        df_b = df.loc[df['ERId'].isin(self.string_list_to_int(diff))]

        workbook = sheet[0] 
        changesbook = sheet[1]
        #only make changes to changes sheet if changes have happened 
        #Doing this to avoid wiping the changes sheet each time you run the software
        if(len(diff) > 0 and len(diff) <= 100000):
            changesbook.clear()
            changesbook.resize(df_b.shape[0], df_b.shape[1])
            changesbook.set_dataframe(df_b, (0,0))
            print(len(diff))
            print("Updated Sheets")
        elif(len(diff) >= 100000):
            print("Probably first time populating wont update sheets changes")

        workbook.clear()
        workbook.resize(df.shape[0], df.shape[1])
        workbook.set_dataframe(df, (0,0))

    def int_list_to_string(self, int_list):
        string_list = [str(x) for x in int_list]
        return string_list

    def string_list_to_int(self, str_list):
        int_list = [int(x) for x in str_list]
        return int_list

    def save_ids(self, df = None):
        '''Takes a dataframe then converts it to a list afterwords stores the ERIds into a text file in CSV format'''
        ERId_s = ""
        if(df is not None):
            ERI_s_list = df['ERId'].tolist()
            #if the list happens to be a list of ints convert it to list of str
            if(type(ERI_s_list[0]) == int):
                ERI_s_list = self.int_list_to_string(df['ERId'].tolist())
            ERId_s = ",".join(ERI_s_list)

        #Checks if there is any changes to ERIds before writting to file
        if(len(self.compare_id(self.ERId, self._ERId_old)) > 0):
            with open('ERId.txt', 'w') as f:
                f.write(ERId_s)
            print("written")    
            
    def compare_id(self, l, l_old):
        '''Finds the difference of 2 lists and returns the indexes as a list'''
        difference = set(l).difference(set(l_old))
       
        return list(difference)

    def retrive_ids(self):
        s = ""
        try:
            with open('ERId.txt', 'r') as f:
                s = str(f.read())
        except:
            with open('ERId.txt', 'w') as f:
                f.write("")
            self.retrive_ids()

        self._ERId_old = s.split(",")

        print(len(self._ERId_old))
    def remove_files(self):
        os.remove(local_file_path)
        os.remove("temp/ExclusionRequests.txt")
        os.removedirs("temp")
    @property
    def tarrif_data(self):
        return self._tariff_data
    @property
    def sheet_name(self):
        return self._sheet_name
    @property
    def zip_file_location(self):
        return self._zip_file_location
    @property 
    def ERId(self):
        return self._ERId
    @property 
    def ERId_old(self):
        return self._ERId_old

if __name__ == "__main__":
    working_dir = os.getcwd()
    print(zipfile36.__version__)
    #gs =  GoogleSheet(url, cert_file, sheet_name, working_dir)
    #gs.download_tariff_zip(local_file_path)
    #gs.extract_zip()
    #print("ex")
    #gs.retrive_ids()
    #print("rt")
    #gs.save_ids(gs.tarrif_data)
    #gs.upload_to_sheets(gs.tarrif_data)
    #gs.remove_files() #Uncomment during production
