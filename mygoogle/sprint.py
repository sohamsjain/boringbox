from os.path import join, dirname

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

SWAYAM_API_KEY = 'Swayam-61479d00dd10.json'


class GoogleSprint:
    service_ac_json_path = join(dirname(__file__), SWAYAM_API_KEY)
    folderMime = 'application/vnd.google-apps.folder'

    def __init__(self):
        self.gs = gspread.service_account(self.service_ac_json_path)
        self.gauth = GoogleAuth()
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.gauth.credentials = ServiceAccountCredentials. \
            from_json_keyfile_name(self.service_ac_json_path, self.scope)
        self.drive = GoogleDrive(self.gauth)
        self.files = self.get_files()

    def get_files(self, query="", show_files=False):
        if query:
            file_list = self.drive.ListFile({'q': query}).GetList()
        else:
            file_list = self.drive.ListFile().GetList()
        if show_files:
            for file1 in file_list:
                print('title: {}, id: {}'.format(file1['title'], file1['id']))
        return pd.DataFrame(file_list)

    def create_folder(self, folder_name, parent_id=None):
        pid = [{'id': parent_id}] if parent_id else []
        folder = self.drive.CreateFile({'title': folder_name, 'parents': pid,
                                        'mimeType': self.folderMime})
        folder.Upload()
        return folder

    @staticmethod
    def share(file, email, role='reader'):
        return file.InsertPermission({'type': 'user', 'value': email,
                                      'role': role})

    def open_by_id(self, _id):
        return self.drive.CreateFile({'id': _id})

    def create_sheet(self, name, folder_id=None):
        return self.gs.create(name, folder_id)

    @staticmethod
    def update_sheet(sheet_obj, df: pd.DataFrame):
        sheet_obj.update([df.columns.values.tolist()] + df.values.tolist())

    @staticmethod
    def fetch_sheet_values(sheet_obj):
        return pd.DataFrame(sheet_obj.get_all_records())

    def files_with_parent_as(self, parent_name):
        return self.get_files(query=f"'{parent_name}' in parents")

    @staticmethod
    def get_file_name(filedf: pd.DataFrame, _id: str):
        try:
            return filedf[filedf.id == _id].title.values[0]
        except:
            return False

    @staticmethod
    def get_file_id(filedf: pd.DataFrame, name: str):
        try:
            return filedf[filedf.title == name].id.values[0]
        except:
            return False
