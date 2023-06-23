from calendarmanager import CalendarManager
import pandas as pd

def main():
    config_path = "config/config.yaml"
    calendar_manager = CalendarManager(config_path)

    # Esempi di utilizzo
    df = pd.read_excel("data/appelli_INL_2022-23.xlsx")
    
    for cod_ad in df["COD_AD"].unique(): 
        print(cod_ad)
        df_ad = df[ df["COD_AD"] == cod_ad ]
        for app in df_ad["APP_ID"].unique():
            print(app)
            df_cds = df_ad[ df_ad["APP_ID"] == app ]
                
            calendar_manager.create_event(df_cds)

if __name__ == "__main__":
    main()