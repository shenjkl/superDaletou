"""Exercise POST routing and authorization without starting a network server."""
from io import BytesIO
import json
from types import SimpleNamespace
import unittest

from dlt.app import Handler


class SupersessionRouteTests(unittest.TestCase):
    def call(self, body, *, origin='http://127.0.0.1:8766', csrf='session-secret'):
        calls = []
        def supersede(reservation_id, *, reason, confirmed):
            calls.append((reservation_id, reason, confirmed))
            if confirmed is not True:
                raise ValueError('confirmation required')
            return {'id': 'new', 'content': {'supersedes': reservation_id}}
        encoded = json.dumps(body).encode('utf-8')
        handler = object.__new__(Handler)
        handler.server = SimpleNamespace(origin='http://127.0.0.1:8766', csrf='session-secret',
                                         fixed_evaluations=SimpleNamespace(supersede_pending=supersede))
        handler.path = '/api/fixed-evaluations/supersede'
        handler.headers = {'Origin': origin, 'Sec-Fetch-Site': 'same-origin', 'X-DLT-CSRF': csrf,
                           'Content-Type': 'application/json', 'Content-Length': str(len(encoded))}
        handler.rfile = BytesIO(encoded)
        handler._validate_host = lambda: True
        handler._session = lambda: True
        handler._discard_bounded_body = lambda: None
        handler._json = lambda status, payload, **kwargs: (status, payload)
        return handler.do_POST(), calls

    def test_route_passes_explicit_confirmation_and_reason(self):
        response, calls = self.call({'reservation_id': 'old', 'reason': 'upgrade', 'confirmed': True})
        self.assertEqual(response, (200, {'reservation': {'id': 'new', 'content': {'supersedes': 'old'}}}))
        self.assertEqual(calls, [('old', 'upgrade', True)])

    def test_cross_site_and_bad_csrf_never_reach_service(self):
        for kwargs in ({'origin': 'https://attacker.invalid'}, {'csrf': 'wrong'}):
            response, calls = self.call({'reservation_id': 'old', 'reason': 'upgrade', 'confirmed': True}, **kwargs)
            self.assertEqual(response[0], 403)
            self.assertEqual(calls, [])

    def test_missing_confirmation_or_fields_are_rejected(self):
        response, calls = self.call({'reservation_id': 'old', 'reason': 'upgrade'})
        self.assertEqual(response[0], 400)
        self.assertEqual(calls, [('old', 'upgrade', None)])
        response, calls = self.call({'reservation_id': 'old', 'confirmed': True})
        self.assertEqual(response[0], 400)
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
