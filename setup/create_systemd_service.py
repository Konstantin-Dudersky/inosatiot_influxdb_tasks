import getpass
import os

path = os.getcwd()

service = f"""[Unit]
Description=Mirror influxdb data
[Service]
Restart=on-failure
RestartSec=30s
Type=simple
User={getpass.getuser()}
Group={getpass.getuser()}
EnvironmentFile=/etc/environment
WorkingDirectory={path}
ExecStart={path}/venv/bin/python3 {path}/main.py --mode rt
[Install]
WantedBy=multi-user.target"""

f = open("setup/inosatiot_influxdb_tasks.service", "w")
f.write(service)
f.close()

print(f'Created service file: \n{service}')
