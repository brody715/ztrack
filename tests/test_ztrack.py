

import json
import time


def test_events_size():
    import msgpack
    metric = {
        'event': 'xxxxxxxx',
        'xxxxx': 'xxxxxxxx',
    }

    for i in range(100):
        metric[f'xxxx{i}'] = 'x' * 20

    data = msgpack.packb(metric, use_bin_type=True)
    print(f"one item msgpack size={len(data)}b")

    print(f"one item json size={len(json.dumps(metric))}b")

    nums = 250 * 100
    metric_list = [metric] * nums

    start = time.time()
    data_msgpack = msgpack.packb(metric_list, use_bin_type=True)
    print(f"{nums} items msgpack size={len(data_msgpack)/10**6}Mb, elapsed={time.time() - start}s")

    start = time.time()
    data_json = json.dumps(metric_list)
    print(f"{nums} items json size={len(data_json)/10**6}Mb, elapsed={time.time() - start}s")

    start = time.time()
    metric_list = msgpack.unpackb(data_msgpack)
    print(f"{nums} items msgpack unpack elapsed={time.time() - start}s")

    start = time.time()
    metric_list = json.loads(data_json)
    print(f"{nums} items json unpack elapsed={time.time() - start}s")
