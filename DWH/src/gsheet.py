import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd

class GSheet:
    def __init__(self, creds_file, spreadsheet_id):
        self._credentials_file = creds_file
        self.spreadsheet_id = spreadsheet_id

        self._service = False
        self._sheets = False

    @property
    def service(self):
        if self._service == False:
            # Читаем ключи из файла
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self._credentials_file,
                                                                           [
                                                                               'https://www.googleapis.com/auth/spreadsheets',
                                                                               'https://www.googleapis.com/auth/drive'])

            http_auth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
            self._service = apiclient.discovery.build('sheets', 'v4',
                                                      http=http_auth)  # Выбираем работу с таблицами и 4 версию API

        return self._service

    @property
    def sheets(self):
        if self._sheets == False:
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheet_list = spreadsheet.get('sheets')  # метаданные
            self._sheets = {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in
                            sheet_list}  # словарь из имен листов и их id

        return self._sheets

    def page(self, page_title):
        if page_title in self.sheets.keys():
            try:
                data = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id,
                                                                range=page_title).execute()
                df = pd.DataFrame(data=data['values'][1:], columns=data['values'][:1])  # данные с листа
                df.columns = [x[0] for x in df.columns]  # сброс мультииндекса
                return df

            except Exception as e:
                print(str(e))
        else:
            print('Нет такого листа')

        return False