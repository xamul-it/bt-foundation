"""Read-only pre-entry account snapshot; stdout contains validated equity only."""
import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path


def main():
    from alpaca.trading.client import TradingClient

    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--mode', required=True, choices=('paper', 'live'))
    args = parser.parse_args()
    key = os.environ.get('ALPACA_API_KEY') or os.environ.get('BROKER_API_KEY')
    secret = os.environ.get('ALPACA_SECRET_KEY') or os.environ.get('BROKER_SECRET_KEY')
    if not key or not secret:
        raise ValueError('Missing account credentials')
    account = TradingClient(key, secret, paper=args.mode == 'paper').get_account()
    equity = float(account.equity)
    if not math.isfinite(equity) or equity <= 0:
        raise ValueError('Pre-entry equity must be finite and positive')
    snapshot = dict(source='alpaca_pre_entry', captured_at=datetime.now(timezone.utc).isoformat(),
                    equity=equity, cash=float(account.cash), buying_power=float(account.buying_power))
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(snapshot, indent=2) + '\n', encoding='utf-8')
    print(equity)


if __name__ == '__main__':
    main()
