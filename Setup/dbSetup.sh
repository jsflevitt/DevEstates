#!/bin/zsh
PATH="/usr/local/sbin:/usr/local/bin:$PATH"

configureSparkNode () {
	local ipAddress=$1
	ssh -i "<key-path>" ubuntu@$ipAddress \
		'cd /usr/local/spark/jars/;
		wget -nv https://jdbc.postgresql.org/download/postgresql-42.2.13.jar;
		exit
		'
}

## Launching postgreSQL EC2 instance
#  image-id Ubuntu 18.0.4 as of June/7/2020
#  block-device-mappings 30GiB Consider the Delete on Termination Instruction
#  subnet determines the VPC
aws ec2 run-instances \
--image-id ami-003634241a8fcdec0 \
--iam-instance-profile Name="dbRole" \
--count 1 \
--instance-type r5a.2xlarge \
--key-name "<key-name>" \
--security-group-ids "<security-group>" \
--subnet-id "<subnet-id>" \
--block-device-mappings "[{\"DeviceName\":\"/dev/sdf\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true}}]" \
--associate-public-ip-address >> dbInstanceLaunchLog

## Assign instance name
dbInstance=$(jq -r '.Instances[0].InstanceId?' dbInstanceLaunchLog)
echo $dbInstance

## Determine instance publicIpAddress
dbPublicIp=$(aws ec2 describe-instances --instance $dbInstance --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo $dbPublicIp

echo "waiting 60 seconds for instance to spin up"
sleep 60

## log into the db machine and proceed to setup postgreSQL server with su postgres
# consider including:
# sudo apt -y upgrade;
# ToDo: consider security and safety implications in:
# 	ssh -o StrictHostKeyChecking=no	and
# 	apt -y 
ssh -o StrictHostKeyChecking=no -i "<key-path>" ubuntu@$dbPublicIp \
'sudo apt -y update;
sudo apt -y install postgresql-10 postgresql postgresql-contrib;
sudo /etc/init.d/postgresql start;
exit
'

echo "Editing configuration"

## log in as user postgres and edit local configuration
#  ToDo: in /etc/postgresql/10/main/postgresql.conf, specify the spark cluster IP addresses & update the port number
ssh -i "<key-path>" ubuntu@$dbPublicIp \
"sudo sed -i -e '59 s/#//1' -e '59 s/localhost/*/1' /etc/postgresql/10/main/postgresql.conf;
sudo sed -i -e '90 a local\tall\t\tall\t\t\t\tmd5' -e '92 a host\tall\t\tall\t\t10.0.0.0/26\tmd5' /etc/postgresql/10/main/pg_hba.conf;
sudo /etc/init.d/postgresql restart;
exit
"

echo "Connecting to spark Cluster"

##	log in to spark Machines and configure to call the db
sparkInstanceMaster=$(jq -r '.Instances[0].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker1=$(jq -r '.Instances[1].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker2=$(jq -r '.Instances[2].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker3=$(jq -r '.Instances[3].InstanceId?' sparkInstanceLaunchLog)


## Determine instance publicIpAddress
sparkMasterPublicIp=$(aws ec2 describe-instances --instance $sparkInstanceMaster --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
sparkWorker1PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker1 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
sparkWorker2PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker2 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
sparkWorker3PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker3 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

configureSparkNode $sparkWorker1PublicIp
configureSparkNode $sparkWorker2PublicIp
configureSparkNode $sparkWorker3PublicIp
configureSparkNode $sparkMasterPublicIp
ssh -i "<key-path>" ubuntu@$sparkMasterPublicIp \
'sed -i "s~spark.jars = ~spark.jars = /usr/local/spark/jars/postgresql-42.2.13.jar, ~" /usr/local/spark/conf/spark-defaults.conf;
sudo -u postgres psql;
CREATE DATABASE zip_code_data;
\q;
sudo apt -y install postgis;
sudo apt -y install postgresql-10-postgis-scripts;
sudo apt -y install postgresql-10-pgrouting;
sudo -u postgres psql -d zip_code_data;
CREATE EXTENSION adminpack;
CREATE SCHEMA postgis;
ALTER DATABASE zip_code_data SET search_path=public, postgis, contrib;
\connect zip_code_data;
CREATE EXTENSION postgis SCHEMA postgis;
\q;
exit
'

echo "postgreSQL DB IP address:\t$dbPublicIp"
