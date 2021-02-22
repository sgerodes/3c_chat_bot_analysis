import json
import re
from collections import defaultdict
from datetime import timedelta

CHAT_HISTORY_JSON_PATH = "resources/chat_export_22_feb.json"
FILTER_ON_TRADES_COUNT = 10 # will filter out all pairs that have trades less then this number. Leave 0 or None for no filter


class CompletedEvent:
    duration = None

    def __init__(self, pair):
        self.pair = pair


class PairAnalysis:
    trades = None
    average_completion_time = None

    def __init__(self, pair):
        self.pair = pair

    def __repr__(self):
        return f"{self.pair}\ttrades={self.trades};\t average completion: {self.average_completion_time}"


def main():
    with open(CHAT_HISTORY_JSON_PATH, "r") as history_file:
        data = json.loads(history_file.read())
        messages = data["messages"]
        durations = defaultdict(list)
        for m in messages:
            if "text" in m:
                try:
                    is_completed_event, event = analyse_message(m["text"])
                    if is_completed_event:
                        durations[event.pair].append(event.duration)
                except Exception as e:
                    print(e)

        for key in durations.keys():
            durations[key] = calculate_average_completion(key, durations[key])

        temp = list(durations.values())
        temp.sort(reverse=False, key=lambda analysis: analysis.average_completion_time)
        if FILTER_ON_TRADES_COUNT:
            temp = filter(lambda an: an.trades > FILTER_ON_TRADES_COUNT, temp)
        for elem in temp:
            print(elem)


def calculate_average_completion(key, dur_list):
    dur_sum = timedelta(seconds=0)
    for d in dur_list:
        split = d.split(" ")
        num = int(split[-2])
        if "minute" in d:
            dur_sum += timedelta(minutes=num)
        elif "hour" in d:
            dur_sum += timedelta(hours=num)
        elif "day" in d:
            dur_sum += timedelta(days=num)
        else:
            raise Exception(f"Unknown time format '{d}'")
        analysis = PairAnalysis(key)
        analysis.trades = len(dur_list)
        analysis.average_completion_time = dur_sum/len(dur_list)
    return analysis


def analyse_message(message_text):
    message_text_str = str(message_text)
    pair = None
    is_deal_completed = False
    duration = None
    if "Deal completed" in message_text_str:
        is_deal_completed = True
        pair = re.search(r"(USDT|BUSD)_\w{1,10}(?=\))", message_text_str)[0]
        duration = message_text[-1]
    event = CompletedEvent(pair)
    event.duration = duration
    return is_deal_completed, event


if __name__ == '__main__':
    main()
