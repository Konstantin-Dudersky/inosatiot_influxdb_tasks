import getpass
import os

path = os.getcwd()

service = f"""[Unit]
Description=Simulator
StartLimitIntervalSec=120
StartLimitBurst=5
[Service]
Restart=on-failure
RestartSec=5s
Type=simple
User={getpass.getuser()}
Group={getpass.getuser()}
EnvironmentFile=/etc/environment
WorkingDirectory={path}
ExecStart={path}/venv/bin/python3 {path}/main.py --mode rt
[Install]
WantedBy=multi-user.target"""

f = open("setup/inosatiot_resources_sim.service", "w")
f.write(service)
f.close()

print(f'Created service file: \n{service}')
