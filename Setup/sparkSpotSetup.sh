#!/bin/zsh
PATH="/usr/local/sbin:/usr/local/bin:$PATH"


##	Machine Environment Setup
#	attempt to minimize interactivity
#	first time accessing a machine so don't ask about ssh fingerprint
#	-y on apt to minimize interactivity
#	remainder required for spark
##	Input:	IP address of machine to be setup
##	Output:	None
setupMachine () {
	local ipAddress=$1
	ssh -o StrictHostKeyChecking=no -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"sudo apt -y update;
		sudo apt -y install openjdk-8-jre-headless;
		sudo apt -y install scala;
		sudo apt -y update;
		sudo apt -y install python-pip;
		pip install py4j;
		exit
		"
}

##	Master machine has extra responsabilities and needs extra software in setup
#	needs to finalize sbt packages
#	openjdk-8-jdk required for that
#	needs to run keyless ssh
##	Input:	IP address of master machine needing additional setup
##	Output:	None
setupMachineMasterAddOn () {
	local ipAddress=$1
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"sudo apt -y install openjdk-8-jdk-headless;
		echo 'deb https://dl.bintray.com/sbt/debian /' | sudo tee -a /etc/apt/sources.list.d/sbt.list;
		curl -sL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823' | sudo apt-key add;
		sudo apt -y update;
		sudo apt -y install sbt;
		sudo apt install openssh-server openssh-client;
		cd /home/ubuntu/.ssh;
		ssh-keygen -t rsa -P '' -f /home/ubuntu/.ssh/id_rsa;
		cat /home/ubuntu/.ssh/id_rsa.pub >> /home/ubuntu/.ssh/authorized_keys;
		exit
		"
}

##	Function to allow master node to communicate securely with workers by keyless ssh
#
##	Input:	IP address of worker then IP address of master
##	Output:	None
sshWorkerSetup () {
	local workerIpAddress=$1
	local masterIpAddress=$2
	ssh -i ~/.ssh/JLevitt-IAM-keypair.pem ubuntu@$masterIpAddress 'cat ~/.ssh/id_rsa.pub' | ssh -i ~/.ssh/JLevitt-IAM-keypair.pem ubuntu@$workerIpAddress 'cat >> ~/.ssh/authorized_keys'
}

##	Function sets up Hadoop and Spark on called node
#	Spark 2.4.5
#	then adds several needed jars:
#	aws-java-sdk-1.7.4.jar
#	hadoop-aws-2.7.7.jar
#	spark-avro2.11-4.0.0.jar
#	wget -nv https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.11/0.13.1/spark-excel_2.11-0.13.1.jar
##	Input:	IP address of node
##	Output:	None	
installSpark () {
	local ipAddress=$1
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"wget -nv '--no-check-certificate' https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz;
		tar xf spark-2.4.5-bin-hadoop2.7.tgz;
		sudo mv spark-2.4.5-bin-hadoop2.7/ /usr/local/spark/;
		rm -r spark-2.4.5-bin-hadoop2.7.tgz;
		sed -i '$ a export PATH=/usr/local/spark/bin:$PATH' ~/.profile;
		source ~/.profile;
		sudo sed -i -e '$ a export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' -e '$ a export JRE_HOME=/usr/lib/jvm/jre' /etc/profile;
		source /etc/profile;
		wget -nv https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar;
		wget -nv https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.7/hadoop-aws-2.7.7.jar;
		wget -nv https://repo1.maven.org/maven2/com/databricks/spark-avro_2.11/4.0.0/spark-avro_2.11-4.0.0.jar;
		sudo mv ~/aws-java-sdk-1.7.4.jar /usr/local/spark/jars/;
		sudo mv ~/hadoop-aws-2.7.7.jar /usr/local/spark/jars/;
		sudo mv ~/spark-avro_2.11-4.0.0.jar /usr/local/spark/jars/;
		exit
		"
}

##	Function sets up node for AWS interfacing
#	Needs redoing for security and flexibility
#	Line number dependence and single profile dependence are concerns
##	Security Note: Stores Keys as plaintext
##	Input:	IP address of node
##	Output:	None	
setupAWSKeys () {
	local ipAddress=$1
	cat $HOME/.aws/credentials | ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress 'cat >> ~/.profile'

	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"sudo sed -i -e '29 s/./#&/' -e '30 s/aws_access_key_id = /export AWS_ACCESS_KEY_ID=/1' -e '31 s/aws_secret_access_key = /export AWS_SECRET_ACCESS_KEY=/1' ~/.profile;
		source ~/.profile;
		exit;
		"
}



##	Launching postgreSQL EC2 instance
#	image-id Ubuntu 20.04 ami-09dd2e08d601bff67 \ # ami-003634241a8fcdec0 18.0.4 as of June/7/2020
#	block-device-mappings 8 GiB Consider the Delete on Termination Instruction
#	subnet determines the VPC
#	associate-public-ip-address means the instance instantiates with an ip address,
#		it can change if the instance is stopped and then started later, Elastic IP can prevent this
# aws ec2 run-instances \
# --image-id ami-003634241a8fcdec0 \
# --iam-instance-profile Name="sparkRole" \
# --count 4 \
# --instance-type t2.large \
# --key-name JLevitt-IAM-keypair \
# --security-group-ids sg-03f52f5b6fa7c1f7f \
# --subnet-id subnet-020a60eeda450819f \
# --block-device-mappings "[{\"DeviceName\":\"/dev/sdf\",\"Ebs\":{\"VolumeSize\":8,\"DeleteOnTermination\":true}}]" \
# --associate-public-ip-address >> sparkInstanceLaunchLog

# Give AWS a moment to spin up instances so remaining calls succeed
# echo "Pausing 1 minute for instances to initialize."
# sleep 60

## Assign Instance Names
aws ec2 describe-instances --filter Name=instance-state-name,Values=running --output json >> sparkInstanceLaunchLog
instances=$(jq -r '.Reservations[0].Instances[].InstanceId?' sparkInstanceLaunchLog)
echo "Instances:\t$instances"

instanceMaster=$(jq -r '.Reservations[0].Instances[0].InstanceId?' sparkInstanceLaunchLog)
instanceWorker1=$(jq -r '.Reservations[0].Instances[1].InstanceId?' sparkInstanceLaunchLog)
instanceWorker2=$(jq -r '.Reservations[0].Instances[2].InstanceId?' sparkInstanceLaunchLog)
instanceWorker3=$(jq -r '.Reservations[0].Instances[3].InstanceId?' sparkInstanceLaunchLog)

## Determine instance publicIpAddress
masterPublicIp=$(aws ec2 describe-instances --instance $instanceMaster --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
worker1PublicIp=$(aws ec2 describe-instances --instance $instanceWorker1 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
worker2PublicIp=$(aws ec2 describe-instances --instance $instanceWorker2 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
worker3PublicIp=$(aws ec2 describe-instances --instance $instanceWorker3 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

##	Setup up machines with language packages
setupMachine $masterPublicIp
setupMachine $worker1PublicIp
setupMachine $worker2PublicIp
setupMachine $worker3PublicIp

##	Setup machines for keyless ssh
#	Master first for ssh and sbt addon to enable package additions
setupMachineMasterAddOn $masterPublicIp
#	Workers
sshWorkerSetup $worker1PublicIp $masterPublicIp
sshWorkerSetup $worker2PublicIp $masterPublicIp
sshWorkerSetup $worker3PublicIp $masterPublicIp


##	Connect the master with all workers
#	find all private ip addresses
masterPrivateIp=$(aws ec2 describe-instances --instance $instanceMaster --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
worker1PrivateIp=$(aws ec2 describe-instances --instance $instanceWorker1 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
worker2PrivateIp=$(aws ec2 describe-instances --instance $instanceWorker2 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
worker3PrivateIp=$(aws ec2 describe-instances --instance $instanceWorker3 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
#	connect
#	again emphasizing no interactivity, but possible security problem
ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$masterPublicIp \
"ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$worker1PrivateIp 'exit';
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$worker2PrivateIp 'exit';
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$worker3PrivateIp 'exit';
exit
"

##	Install Spark on each machine
installSpark $masterPublicIp
installSpark $worker1PublicIp
installSpark $worker2PublicIp
installSpark $worker3PublicIp


##	Allow machines to use AWS Keys
setupAWSKeys $masterPublicIp
setupAWSKeys $worker1PublicIp
setupAWSKeys $worker2PublicIp
setupAWSKeys $worker3PublicIp

##	Learn machine public DNS for configuration
masterPublicDNS=$(aws ec2 describe-instances --instance $instanceMaster --query 'Reservations[0].Instances[0].PublicDnsName' --output text)


##	Now use Master to spin up the cluster
#	ssh lines 1-8 edit/setup the Hadoop configuration files
#	ssh lines 9-15 synchronise Hadoop to run its name and datanodes
#	ssh lines 16-21 setup the spark configuration files
# -e '$ a export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64'
ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$masterPublicIp \
"cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh;
sed -i -e 's/# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname/export SPARK_MASTER_HOST=$masterPrivateIp/' -e '$ a export JAVA_HOME=/usr' -e '$ a export PYSPARK_PYTHON=python3' /usr/local/spark/conf/spark-env.sh;
cp /usr/local/spark/conf/slaves.template /usr/local/spark/conf/slaves;
sed -i -e 's/localhost/$worker1PrivateIp/' -e '$ a $worker2PrivateIp' -e '$ a $worker3PrivateIp' /usr/local/spark/conf/slaves;
cp /usr/local/spark/conf/spark-defaults.conf.template /usr/local/spark/conf/spark-defaults.conf;
sed -i -e '$ a spark.jars = /usr/local/spark/jars/hadoop-aws-2.7.7.jar, /usr/local/spark/jars/aws-java-sdk-1.7.4.jar, /usr/local/spark/jars/spark-avro_2.11-4.0.0.jar' /usr/local/spark/conf/spark-defaults.conf;
exit
"

echo "master IP address:\t$masterPublicIp"
echo "worker 1 IP address:\t$worker1PublicIp"
echo "worker 2 IP address:\t$worker2PublicIp"
echo "worker 3 IP address:\t$worker3PublicIp"


echo "Recommended Next Steps To Complete Cluster Setup:
	ssh -i <keypairPath> ubuntu@$masterPublicIp
	cd /usr/local/spark
	sbin/start-all.sh"

# master IP address:	34.216.251.49
# worker 1 IP address:	54.218.85.160
# worker 2 IP address:	34.220.80.79
# worker 3 IP address:	34.221.66.68

##	Python Note: 
#	sudo apt-get install -y python3-pip
#	export PYTHONPATH=/usr/lib/python3.5		##Check version
#	export PYSPARK_PYTHON=/usr/bin/python3.5	##Check version