# Dashboard/API dev-prod split

## Goal

- `dev`: dashboard su `localhost`, backend su `localhost`
- `prod`: dashboard pubblicata su `ilz.duckdns.org`, backend pubblicato tramite reverse proxy
- DB condiviso: `env/bt-live-events`

## Env files

- API dev: `env/bt-api-dev`
- API prod: `env/bt-api-prod`
- Server policy dev: `env/server-dev`
- Server policy prod: `env/server-prod`
- Dash dev: `env/bt-dash-dev`
- Dash prod: `env/bt-dash-prod`

## Ports

- Dev API: `127.0.0.1:9090`
- Dev Dash: `127.0.0.1:8082`
- Prod API: `127.0.0.1:19090`
- Prod Dash: `127.0.0.1:18082`

## systemd

Reload:

```bash
systemctl --user daemon-reload
```

Start dev:

```bash
systemctl --user start bt-api@dev.service
systemctl --user start bt-dash@dev.service
```

Start prod:

```bash
systemctl --user start bt-api@prod.service
systemctl --user start bt-dash@prod.service
```

Legacy aliases:

- `bt-api.service` now loads `env/bt-api-dev`
- `bt-dash.service` now loads `env/bt-dash-dev`

## Reverse proxy

Sample nginx config:

- `scripts/ilz.duckdns.org.nginx.conf`

Expected public routes:

- `https://ilz.duckdns.org/api`
- `https://ilz.duckdns.org/dash`
