import os
import yaml
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from loguru import logger
from google.oauth2.credentials import Credentials
import datetime
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
SCOPES = ["https://www.googleapis.com/auth/calendar"]
# SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

event_struct =  {
    # COD_AD + APP_ID
    'id': "",
    'summary': "",
    'location': "",
    'description': "",
    
    'start': {
        "date": ""
    #    'dateTime': "",
    #    'timeZone': "",
    },
    
    'end': {
        "date": ""
    #    'dateTime': "",
    #    'timeZone': "",
    },
    
    
    'attendees': [
        # Nome Cognome Matricola
        { 'displayName': ''},
    ],

}
        

class CalendarManager:
    def __init__(self, config_path):
        self.event_struct = event_struct.copy()
        
        self.config = self._load_config(config_path)
        logger.info(self.config)
        
        #self.event_struct["start"]["timeZone"] = self.config["timezone"]
        #self.event_struct["end"]["timeZone"] = self.config["timezone"]
        
        self.credentials = None
        self.service = None

        
    def _load_config(self, config_path):
        with open(config_path, "r") as config_file:
            return yaml.safe_load(config_file)
    
    def _authenticate(self):
        credentials_path = self.config["credentials_path"]
        self.credentials = None

        if os.path.exists(self.config["token_path"]):
            self.credentials = Credentials.from_authorized_user_file(self.config["token_path"], SCOPES)

        # If there are no (valid) credentials available, let the user log in.
        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES)
                self.credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.config["token_path"], 'w') as token:
                token.write(self.credentials.to_json())
   
    def _create_event(self, id, summary, description, start_time, end_time, location, attendees):
        
        event = self.event_struct.copy()
        
        event["id"] = id
        event["summary"] = summary
        event["description"] = description
        event["start"]["date"] = start_time
        event["end"]["date"] = end_time
        event["location"] = location
        event["attendees"] = attendees 
        
        logger.info(event)

        try:
            event = self.service.events().insert(calendarId=self.config["calendar_id"], body=event, sendUpdates = None).execute()
        except:
            event = self.service.events().update(calendarId=self.config["calendar_id"], eventId = id, body=event, sendUpdates = None).execute()
            
        logger.info('Event created: %s' % (event.get('htmlLink')) )
    
    @logger.catch
    def _delete_event(self, id):
        event = self.service.events().delete(calendarId=self.config["calendar_id"], eventId = id, sendUpdates = None).execute()
        logger.info('Event deleted: %s' % (id)) 
        
    def _parse_df(self, df):    
        df = df.drop_duplicates()
        print(df)
        
        
        
        id = str(df["COD_AD"].unique()[0]) + str(df["APP_ID"].unique()[0])  
        id = id.lower()
        
        summary = str(df["DES_AD"].unique()[0]) + " " + str(df["DES_CDS"].unique()[0])  
        
        try:
            description = "Studenti prenotati %d/%d" % (int(df["STUDENTI_PRENOTATI"].unique()[0]), int(df["NUMERO_MAX"].unique()[0]) ) 
        except:
            description = "Studenti prenotati %d/inf" % (int(df["STUDENTI_PRENOTATI"].unique()[0]) )
        
        dd, mm, yy = str(df["DATA_ESA"].unique()[0]).split("/")
        yy = yy.split(" ")[0]
         
        end_time = yy + "-" + mm + "-" + dd
        start_time = yy + "-" + mm + "-" + dd

        location =  str(df["LUOGO"].unique()[0])
        
        attendees = []
        for i in range(len(df)):
            row = { "displayName": df["NOME"].values[i] + " " + df["COGNOME"].values[i] + " " + str(df["MATRICOLA"].values[i]) + ", " +  str(df["RUOLO"].values[i]),
                    "email": "__" + df["NOME"].values[i].lower().replace(" ", "") + "." + df["COGNOME"].values[i].lower().replace(" ", "") + "@unifi.it"
                    }
            attendees.append(row)
            
        return id, summary, description, start_time, end_time, location, attendees
    
    def create_event(self, df):
        if not self.service:
            self._authenticate()
            logger.info(self.credentials)
            self.service = build("calendar", "v3", credentials=self.credentials)

        id, summary, description, start_time, end_time, location, attendees = self._parse_df(df)
    
        self._create_event(id, summary, description, start_time, end_time, location, attendees)
