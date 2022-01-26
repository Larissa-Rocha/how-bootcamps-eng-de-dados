import datetime
import time

from schedule import repeat, every, run_pending
from ingestors import DaySummaryIngestor
from writers import DataWriter


if __name__ == "__main__":
    day_summary_ingestor = DaySummaryIngestor(
        writer=DataWriter,
        coins=["BTC", "ETH", "LTC", "BCH"],
        default_start_date=datetime.date(2021, 6, 1)
    )

    #trades_ingestor = TradesIngestor()


    @repeat(every(1).seconds)
    def job():
        day_summary_ingestor.ingest()
        #trades_ingestor.ingest()


    while True:
        run_pending()
        time.sleep(0.5)


