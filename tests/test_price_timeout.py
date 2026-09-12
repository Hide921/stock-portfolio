"""通信を使わず、価格取得の期限と部分結果を確認する。"""
import ast
import concurrent.futures
import logging
from pathlib import Path
import threading
import time
import unittest


class PriceTimeoutTest(unittest.TestCase):
    def test_partial_result_returns_without_waiting_for_running_task(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'server.py').read_text(encoding='utf-8'))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_run_pool')
        scope = {'concurrent': concurrent, 'logging': logging}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'server.py', 'exec'), scope)
        release = threading.Event()

        def fetch(ticker):
            if ticker == 'SLOW':
                release.wait(2)
            return {'price': 100}

        try:
            started = time.monotonic()
            ok, errors = scope['_run_pool'](['FAST', 'SLOW'], 2, 0.05, 0.01, fetcher=fetch)
            self.assertLess(time.monotonic() - started, 1)
            self.assertEqual(ok['FAST']['price'], 100)
            self.assertEqual(errors['SLOW'], 'timeout')
        finally:
            release.set()


if __name__ == '__main__':
    unittest.main()
