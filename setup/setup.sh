#!/bin/bash

echo
echo "-----> Python version:"
python3.9 -V

#echo
#echo "-----> Updating system:"
#sudo apt update
#sudo apt -y upgrade

echo
echo "-----> Install python base packages:"
sudo apt install -y python3-pip
sudo apt install -y python3-venv

echo
echo "-----> Create virtual environment:"
python3.9 -m venv venv
source venv/bin/activate
python3.9 -m pip install -r setup/requirements.txt

#echo
#echo "-----> Create systemd service:"
#python3.9 setup/create_systemd_service.py
#sudo mv setup/inosatiot_resources_sim.service /etc/systemd/system
#sudo systemctl daemon-reload
#sudo systemctl enable inosatiot_resources_sim.service

#echo
#echo "-----> Start:"
#sudo systemctl start inosatiot_resources_sim.service

echo
echo "-----> Finish!"
