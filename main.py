import json
import re
from collections import defaultdict
from datetime import timedelta, datetime
import logging
import yaml
from enum import Enum


class SortBy(Enum):
    AVERAGE_COMPLETION_TIME = "AVERAGE_COMPLETION_TIME"
    AVERAGE_PROFIT_PER_HOUR = "AVERAGE_PROFIT_PER_HOUR"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CHAT_HISTORY_JSON_PATH = "resources/chat_export_1_march.json"
TRADES_LOWER_BOUND_FILTER = 10
START_DATE_BOUND_FILTER = None #"1021-02-19T00:00:00"
END_DATE_BOUND_FILTER = "3021-02-22T00:00:00"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
PRIMARY_COINS = ["BUSD"] # could be multiple like ["BUSD", "USDT", "BTC", "ETH"]
SORT_BY = SortBy.AVERAGE_COMPLETION_TIMEa



class CompletedEvent:
    duration = None
    profit = None

    def __init__(self, pair):
        self.pair = pair

    def __repr__(self):
        return f"{self.pair}; {self.profit}; {self.duration}"


class PairAnalysis:
    average_completion_time = None
    average_profit = None
    average_ratio = None

    def __init__(self, pair):
        self.profits_list = list()
        self.durations_list = list()
        self.pair = pair

    def __repr__(self):
        delta_without_microseconds = str(self.average_completion_time).split('.')[0]
        pair_tabbed = self.pair if len(self.pair) > 7 else self.pair + "\t"
        formatted_profit = "{:.3f}".format(self.average_profit)
        formatted_ratio = "{:.3f}".format(self.average_ratio)
        return f"{pair_tabbed}\ttrades={self.get_trades_count()};\t avg completion: {delta_without_microseconds};\t avg profit: {formatted_profit};\t profit/h: {formatted_ratio}"

    def get_trades_count(self):
        return len(self.durations_list)

    def add_duration_as_string(self, dur):
        self.durations_list.append(self.str_to_timedelta(dur))

    def add_profit(self, profit):
        self.profits_list.append(profit)

    def calculate_average_duration(self):
        s = timedelta(seconds=0)
        for d in self.durations_list:
            s += d
        self.average_completion_time = s/len(self.durations_list)

    def calculate_average_profit(self):
        s = 0
        for p in self.profits_list:
            s += float(p)
        self.average_profit = s/len(self.profits_list)

    def calculate_ratio(self):
        if not self.average_profit or not self.average_completion_time:
            raise Exception("must have both average_profit and average_completion_time for calculating the ratio")
        # per hour
        self.average_ratio = self.average_profit / self.average_completion_time.total_seconds()*60*60

    @staticmethod
    def str_to_timedelta(d):
        split = d.split(" ")
        num = int(split[-2])
        if "minute" in d:
            return timedelta(minutes=num)
        elif "hour" in d:
            return timedelta(hours=num)
        elif "day" in d:
            return timedelta(days=num)
        else:
            raise Exception(f"Unknown time format '{d}'")


def main():
    if TRADES_LOWER_BOUND_FILTER:
        print(f"Filter out pairs with trades < {TRADES_LOWER_BOUND_FILTER}")
    if START_DATE_BOUND_FILTER:
        print(f"Filter out trades before {START_DATE_BOUND_FILTER}")
    print(f"Analysing pairs with base coin(s): {PRIMARY_COINS}")
    with open(CHAT_HISTORY_JSON_PATH, "r") as history_file:
        data = json.loads(history_file.read())
        messages = data["messages"]
        pair_analysis_storage = dict()
        start_date_filter = datetime.strptime(START_DATE_BOUND_FILTER, DATE_FORMAT) if START_DATE_BOUND_FILTER else None
        end_date_filter = datetime.strptime(END_DATE_BOUND_FILTER, DATE_FORMAT) if END_DATE_BOUND_FILTER else None
        for m in messages:
            if "date" not in m:
                continue
            timestamp = datetime.strptime(m["date"], DATE_FORMAT)
            if start_date_filter and timestamp < start_date_filter:
                continue
            if end_date_filter and timestamp > end_date_filter:
                continue
            if "text" in m:
                try:
                    is_completed_event, event = analyse_message(m["text"])
                    if is_completed_event:
                        if event.pair not in pair_analysis_storage:
                            pair_analysis_storage[event.pair] = PairAnalysis(event.pair)
                        pair_analysis_storage[event.pair].add_duration_as_string(event.duration)
                        pair_analysis_storage[event.pair].add_profit(event.profit)
                except Exception as e:
                    logger.debug(e)

        all_pairs = list(pair_analysis_storage.values())
        if TRADES_LOWER_BOUND_FILTER:
            all_pairs = list(filter(lambda an: an.get_trades_count() >= TRADES_LOWER_BOUND_FILTER, all_pairs))
        for p in all_pairs:
            p.calculate_average_duration()
            p.calculate_average_profit()
            p.calculate_ratio()
        # all_pairs = list(filter(lambda an: an.average_profit < 0.5, all_pairs))
        print(SORT_BY)
        if SORT_BY is SortBy.AVERAGE_COMPLETION_TIME:
            all_pairs.sort(reverse=False, key=lambda analysis: analysis.average_completion_time)
        elif SORT_BY is SortBy.AVERAGE_PROFIT_PER_HOUR:
            all_pairs.sort(reverse=True, key=lambda analysis: analysis.average_ratio)
        for p in all_pairs:
            print(p)


def analyse_message(message_text):
    message_text_str = str(message_text)
    pair = None
    price = None
    is_deal_completed = False
    duration = None
    if "Deal completed" in message_text_str:
        is_deal_completed = True
        base_coins = "(" + '|'.join(PRIMARY_COINS) + ")"
        pair = re.search(base_coins + r"_\w{1,10}(?=\))", message_text_str)[0]
        price = re.search(r"[+\- ]\d+\.\d{0,8}(?= " + base_coins + ")", message_text_str)[0]
        duration = message_text[-1]
    event = CompletedEvent(pair)
    event.duration = duration
    event.profit = price
    return is_deal_completed, event


if __name__ == '__main__':
    main()
