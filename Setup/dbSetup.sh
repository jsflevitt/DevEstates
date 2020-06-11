#!/bin/zsh
PATH="/usr/local/sbin:/usr/local/bin:$PATH"

configureSparkNode () {
	local ipAddress=$1
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
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
--instance-type m4.large \
--key-name JLevitt-IAM-keypair \
--security-group-ids sg-03f52f5b6fa7c1f7f \
--subnet-id subnet-020a60eeda450819f \
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
ssh -o StrictHostKeyChecking=no -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$dbPublicIp \
'sudo apt -y update;
sudo apt -y install postgresql-10 postgresql postgresql-contrib;
sudo /etc/init.d/postgresql start;
exit
'

echo "Editing configuration"

## log in as user postgres and edit local configuration
#  ToDo: in /etc/postgresql/10/main/postgresql.conf, specify the spark cluster IP addresses & update the port number
ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$dbPublicIp \
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

configureSparkNode $sparkMasterPublicIp
configureSparkNode $sparkWorker1PublicIp
configureSparkNode $sparkWorker2PublicIp
configureSparkNode $sparkWorker3PublicIp

echo "postgreSQL DB IP address:\t$dbPublicIp"

# 34.220.79.22


##	Next steps:
#	log back in
# ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$dbPublicIp
# sudo -u postgres psql
# postgres=# \password;
##	input password twice
# postgres=# CREATE DATABASE <database_name>;

##	Figure out how to successfully do the following:
##	Create User for connecting to the DB from outside
##	CREATE USER db_select WITH PASSWORD '<See Password Manager>';  ## No Symbols
##	grant the ability to create and manipulate tables (consider limits)
##	GRANT ALL PRIVILEGES ON DATABASE postgres TO db_select;
