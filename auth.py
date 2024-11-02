import gspread
from oauth2client.service_account import ServiceAccountCredentials
from decouple import config

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(config('file_auth'), scope)
gc = gspread.authorize(credentials)

spreadsheet_id = "1tWcWp2d5224ZHq4SNzdyhmkorNUGxlPbw6TYj7q_0Wo"
spreadsheet = gc.open_by_key(spreadsheet_id)

worksheet = spreadsheet.worksheet('(for BI) data')