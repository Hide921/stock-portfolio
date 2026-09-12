"""即時応答とバックグラウンド取得の上限を外部通信なしで検証する。"""
import ast
import logging
from pathlib import Path
import threading
import unittest
from unittest.mock import Mock


def functions(names, scope):
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'server.py').read_text(encoding='utf-8'))
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=body, type_ignores=[]), 'server.py', 'exec'), scope)


class ImmediatePricesTest(unittest.TestCase):
    def test_returns_saved_values_and_schedules_missing_only(self):
        refresh = Mock()
        scope = {
            '_cached_price': lambda t: {'price': 10} if t == 'FRESH' else None,
            '_last_known_price': lambda t: {'price': 9, 'stale': True} if t == 'OLD' else None,
            '_trigger_background_refresh': refresh,
        }
        functions(['_get_prices_immediate'], scope)
        result = scope['_get_prices_immediate'](['FRESH', 'OLD', 'MISSING'])
        self.assertEqual(result['FRESH']['price'], 10)
        self.assertTrue(result['OLD']['stale'])
        self.assertTrue(result['MISSING']['pending'])
        self.assertEqual([c.args[0] for c in refresh.call_args_list], ['OLD', 'MISSING'])

    def test_background_work_has_limit_and_releases_slot(self):
        workers = []
        fake_threading = Mock()
        fake_threading.Thread.side_effect = lambda target, daemon: workers.append(target) or Mock()
        scope = {
            '_refresh_lock': threading.Lock(), '_refresh_in_flight': set(),
            '_refresh_slots': threading.BoundedSemaphore(1), 'threading': fake_threading,
            'logging': logging, 'is_fund_ticker': lambda t: False,
            'fetch_price_with_retry': lambda t: {'price': 10}, '_store_price': Mock(),
        }
        functions(['_trigger_background_refresh'], scope)
        trigger = scope['_trigger_background_refresh']
        trigger('A')
        trigger('A')
        trigger('B')
        self.assertEqual(len(workers), 1)
        workers[0]()
        trigger('B')
        self.assertEqual(len(workers), 2)
        workers[1]()
        self.assertEqual(scope['_refresh_in_flight'], set())


if __name__ == '__main__':
    unittest.main()
