"""Isolated authenticated preview of the real knowledge API and a temporary PWA build."""
import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'bt-api'), str(ROOT / 'bt-knowledge')]

from flask import Flask, send_from_directory
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.security import generate_password_hash
from werkzeug.serving import run_simple


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--port', type=int, default=19691)
    args = parser.parse_args()
    os.environ['BT_DASH_SECRET_KEY'] = 'isolated-knowledge-preview-not-production'
    os.environ['BT_DASH_PW_HASH'] = generate_password_hash('knowledge-preview', method='pbkdf2:sha256')
    from app.auth import init_auth
    from app.knowledge import knowledge_bp
    api = Flask('knowledge-preview-api')
    api.register_blueprint(knowledge_bp, url_prefix='/dyn/knowledge')
    init_auth(api)
    api.config['SESSION_COOKIE_SECURE'] = False

    @api.get('/dyn/sc/status')
    def scheduler_status():
        return {'status': 'stopped'}  # Preview has no scheduler.

    static = Flask('knowledge-preview-static', static_folder=None)

    @static.get('/dash/')
    @static.get('/dash/<path:filename>')
    def files(filename='index.html'):
        return send_from_directory(args.build, filename)

    app = DispatcherMiddleware(static, {'/api': api})
    run_simple('127.0.0.1', args.port, app, threaded=True, use_reloader=False)


if __name__ == '__main__':
    main()
