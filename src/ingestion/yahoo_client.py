import yfinance as yf


class YahooClient:
    def get_daily_prices(self, symbol, start_date, end_date=None):
        ticker = yf.Ticker(symbol)

        frame = ticker.history(
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
        )

        frame = frame.reset_index()
        raw_rows = frame.to_dict(orient="records")
        records = []

        for row in raw_rows:
            normalized_row = {}

            for key, value in row.items():
                field_name = str(key)

                if hasattr(value, "isoformat"):
                    field_value = value.isoformat()
                else:
                    field_value = value

                normalized_row[field_name] = field_value

            records.append(normalized_row)

        payload = {
            "source": "yahoo",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "interval": "1d",
            "records": records,
        }

        return payload
