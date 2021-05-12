"""
python code book chapter 1
"""
import heapq


class PriorityQueue:
    """利用heapq实现优先队列"""

    def __init__(self):
        self._queue = []
        self._index = 0

    def push(self, item, priority):
        """(-priority, self._index, item) priority 优先级  self._index 元素的索引  item 元素本身"""
        heapq.heappush(self._queue, (-priority, self._index, item))
        self._index += 1

    def pop(self):
        """取出优先最大的元素"""
        return heapq.heappop(self._queue)[-1]


def get_month_range(start_date=None, to_str=True):
    """计算给定日期对应的月初和月末日期"""
    from datetime import timedelta, date
    import calendar
    if isinstance(start_date, str):
        start_date = [int(val) for val in start_date.split('-')][:2]
        start_date = date(*start_date, day=1)
    if not start_date:
        start_date = date.today().replace(day=1)

    _, days = calendar.monthrange(start_date.year, start_date.month)
    end_date = start_date + timedelta(days=days - 1)
    return str(start_date), str(end_date) if to_str else (start_date, end_date)


if __name__ == '__main__':
    print(get_month_range('2020-02-02'))
