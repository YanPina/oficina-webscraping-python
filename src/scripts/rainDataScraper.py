import requests
import pandas as pd

from io import StringIO
from bs4 import BeautifulSoup


class RainDataScraper:
    def __init__(self, start_date: str, end_date: str) -> None:
        self.start_date = start_date
        self.end_date = end_date

        self.base_url: str = "http://www.ciiagro.org.br/diario/periodo"
        self.query_url: str = "http://www.ciiagro.org.br/diario/cperiodo"

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Length": "61",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        self.stations_id = self.__get_stations_id()

        self.__main()

    def __main(self) -> None:
        df = pd.DataFrame()
        
        for station_id in self.stations_id:
            station_data = self.__get_data(station_id)

            df = pd.concat([station_data, df], ignore_index=True)

        print(df)
        df.to_excel("data.xlsx")

    def __get_stations_id(self) -> list[int]:
        response = requests.get(url=self.base_url)

        soup = BeautifulSoup(response.content, 'html.parser')

        checkboxes = soup.find_all('input', type='checkbox')

        return [int(checkbox.get('value')) for checkbox in checkboxes if
                checkbox.get('value')]


    def __get_data(self, station_id: int) -> pd.DataFrame:
        data = {
            "estacao_id[]": [station_id],
            "inicio": self.start_date,
            "final": self.end_date
        }
        try:
            response = requests.post(
                url=self.query_url, 
                headers=self.headers, 
                data=data)

            print(f'station: {station_id} successful!')
            return self.__parse_data(response.text)

        except Exception as error:
            print(f"Estacao {station_id} com erro: ", error)
            return pd.DataFrame()

    def __parse_data(self, response_text: str) -> pd.DataFrame:
        tables = pd.read_html(StringIO(response_text))

        df_station = pd.DataFrame(tables[1])
        df_station.columns = df_station.columns.droplevel(0)

        return df_station