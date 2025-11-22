from datetime import date, timedelta
from typing import Tuple, Iterator
from dateutil.relativedelta import relativedelta

def get_report_date_range(strategy: str) -> Tuple[date, date]:
    today = date.today()

    if strategy == 'today_only':
        return (today, today)

    if strategy == 'month_to_date':
        start_of_month = today.replace(day=1)
        return (start_of_month, today)

    if strategy == 'previous_month':
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
        return (first_day_of_previous_month, last_day_of_previous_month)

    return (today, today)

def generate_date_batches(start_date: date, end_date: date, batch_days: int) -> Iterator[Tuple[str, str]]:
    current_start = start_date
    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=batch_days - 1),
            end_date
        )

        yield (
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        )

        current_start = current_end + timedelta(days=1)