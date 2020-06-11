#!/bin/zsh
PATH="/usr/local/sbin:/usr/local/bin:$PATH"


##	Machine Environment Setup
#	attempt to minimize interactivity
#	first time accessing a machine so don't ask about ssh fingerprint
#	-y on apt to minimize interactivity
#	openjdk-8-openjdk required by hadoop too
#	remainder required for spark
##	Input:	IP address of machine to be setup
##	Output:	None
setupMachine () {
	local ipAddress=$1
	ssh -o StrictHostKeyChecking=no -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"sudo apt -y update;
		sudo apt -y install openjdk-8-jdk;
		sudo apt -y install openjdk-8-jre-headless;
		sudo apt -y install scala;
		sudo apt -y update;
		sudo apt -y install python-pip;
		pip install py4j;
		exit
		"
}

##	Master machine has extra responsabilities and needs extra software in setup
#	needs to run keyless ssh
#	needs to finalize sbt packages
##	Input:	IP address of master machine needing additional setup
##	Output:	None
setupMachineMasterAddOn () {
	local ipAddress=$1
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"echo 'deb https://dl.bintray.com/sbt/debian /' | sudo tee -a /etc/apt/sources.list.d/sbt.list;
		curl -sL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823' | sudo apt-key add;
		sudo apt -y update;
		sudo apt -y install sbt;
		sudo apt install openssh-server openssh-client;
		cd ~/.ssh;
		ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa;
		cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys;
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
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$masterIpAddress 'cat ~/.ssh/id_rsa.pub' | ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@workerIpAddress 'cat >> ~/.ssh/authorized_keys'
}

##	Function sets up Hadoop and Spark on called node
#	Hadoop 2.7.7
#	Spark 2.4.5
#	then adds several needed jars:
#	aws-java-sdk-1.7.4.jar
#	hadoop-aws-2.7.7.jar
#	spark-avro2.11-4.0.0.jar
##	Input:	IP address of node
##	Output:	None	
installHadoopAndSpark () {
	local ipAddress=$1
	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"wget -nv '--no-check-certificate' http://apache.mirrors.hoobly.com/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz;
		tar xf hadoop-2.7.7.tar.gz;
		sudo mv hadoop-2.7.7 /usr/local/hadoop;
		wget -nv '--no-check-certificate' https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz;
		tar xf spark-2.4.5-bin-hadoop2.7.tgz;
		sudo mv spark-2.4.5-bin-hadoop2.7/ /usr/local/spark/;
		sed -i '$ a export PATH=/usr/local/hadoop/bin:/usr/local/spark/bin:$PATH' ~/.profile;
		source ~/.profile;
		sudo sed -i -e '$ a export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk' -e '$ a export JRE_HOME=/usr/lib/jvm/jre' /etc/profile;
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
	cat ~/.aws/credentials | ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress 'cat >> ~/.profile'

	ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$ipAddress \
		"sudo sed -i -e '29 s/./#&/' -e '30 s/aws_access_key_id = /export AWS_ACCESS_KEY_ID=/1' -e '31 s/aws_secret_access_key = /export AWS_SECRET_ACCESS_KEY=/1' ~/.profile;
		source ~/.profile;
		exit;
		"
}



## Launching postgreSQL EC2 instance
#  image-id Ubuntu 20.04 ami-09dd2e08d601bff67 \ # ami-003634241a8fcdec0 18.0.4 as of June/7/2020
#  block-device-mappings 8 GiB Consider the Delete on Termination Instruction
#  subnet determines the VPC
aws ec2 run-instances \
--image-id ami-003634241a8fcdec0 \
--iam-instance-profile Name="sparkRole" \
--count 4 \
--instance-type t2.large \
--key-name JLevitt-IAM-keypair \
--security-group-ids sg-03f52f5b6fa7c1f7f \
--subnet-id subnet-020a60eeda450819f \
--block-device-mappings "[{\"DeviceName\":\"/dev/sdf\",\"Ebs\":{\"VolumeSize\":8,\"DeleteOnTermination\":true}}]" \
--associate-public-ip-address >> sparkInstanceLaunchLog

# Give AWS a moment to spin up instances so remaining calls succeed
echo "Pausing 1 minute for instances to initialize."
sleep 60

## Assign Instance Names
sparkInstances=$(jq -r '.Instances[].InstanceId?' sparkInstanceLaunchLog)
echo $sparkInstances

sparkInstanceMaster=$(jq -r '.Instances[0].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker1=$(jq -r '.Instances[1].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker2=$(jq -r '.Instances[2].InstanceId?' sparkInstanceLaunchLog)
sparkInstanceWorker3=$(jq -r '.Instances[3].InstanceId?' sparkInstanceLaunchLog)

## Determine instance publicIpAddress
sparkMasterPublicIp=$(aws ec2 describe-instances --instance $sparkInstanceMaster --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo $sparkMasterPublicIp
sparkWorker1PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker1 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
sparkWorker2PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker2 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
sparkWorker3PublicIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker3 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo $sparkWorker1PublicIp, $sparkWorker2PublicIp, $sparkWorker3PublicIp

##	Setup up machines with language packages
setupMachine $sparkMasterPublicIp
setupMachine $sparkWorker1PublicIp
setupMachine $sparkWorker2PublicIp
setupMachine $sparkWorker3PublicIp

##	Setup machines for keyless ssh
#	Master first for ssh and sbt addon to enable package additions
setupMachineMasterAddOn $sparkMasterPublicIp
#	Workers
sshWorkerSetup $sparkWorker1PublicIp $sparkMasterPublicIp
sshWorkerSetup $sparkWorker2PublicIp $sparkMasterPublicIp
sshWorkerSetup $sparkWorker3PublicIp $sparkMasterPublicIp


##	Connect the master with all workers
#	find all private ip addresses
sparkMasterPrivateIp=$(aws ec2 describe-instances --instance $sparkInstanceMaster --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
sparkWorker1PrivateIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker1 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
sparkWorker2PrivateIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker2 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
sparkWorker3PrivateIp=$(aws ec2 describe-instances --instance $sparkInstanceWorker3 --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
#	connect
#	again emphasizing no interactivity, but possible security problem
ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$sparkMasterPublicIp \
"ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$sparkWorker1PrivateIp 'exit';
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$sparkWorker2PrivateIp 'exit';
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@$sparkWorker3PrivateIp 'exit';
exit
"

##	Install Spark and Hadoop on each machine
installHadoopAndSpark $sparkMasterPublicIp
installHadoopAndSpark $sparkWorker1PublicIp
installHadoopAndSpark $sparkWorker2PublicIp
installHadoopAndSpark $sparkWorker3PublicIp


##	Allow machines to use AWS Keys
setupAWSKeys $sparkMasterPublicIp
setupAWSKeys $sparkWorker1PublicIp
setupAWSKeys $sparkWorker2PublicIp
setupAWSKeys $sparkWorker3PublicIp

##	Learn machine public DNS for configuration
sparkMasterPublicDNS=$(aws ec2 describe-instances --instance $sparkInstanceMaster --query 'Reservations[0].Instances[0].PublicDnsName' --output text)


##	Now use Master to spin up the cluster
ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$sparkMasterPublicIp \
"sed -i -e '25 s/./#&/' -e '26 i export JAVA_HOME=/usr' -e '$ a export HADOOP_CLASSPATH=/usr/local/hadoop/share/hadoop/tools/lib/*:\$HADOOP_CLASSPATH' /usr/local/hadoop/etc/hadoop/hadoop-env.sh;
sed -i '20 i \n<property>\n<name>fs.defaultFS</name>\n<value>hdfs://$sparkMasterPublicDNS:9000</value>\n</property>\n\n<property>\n<name>fs\.s3\.impl</name>\n<value>org\.apache\.hadoop\.fs\.s3a\.S3AFileSystem</value>\n</property>' /usr/local/hadoop/etc/hadoop/core-site.xml;
sed -i '18 i \n<property>\n<name>yarn\.nodemanager\.aux-services</name> <value>mapreduce_shuffle</value>\n</property>\n<property>\n<name>yarn\.nodemanager\.aux-services\.mapreduce\.shuffle\.class</name>\n<value>org\.apache\.hadoop\.mapred\.ShuffleHandler</value>\n</property>\n<property>\n<name>yarn\.resourcemanager\.resource-tracker\.address</name>\n<value>$sparkMasterPublicDNS:8025</value>\n</property>\n<property>\n<name>yarn\.resourcemanager\.scheduler\.address</name>\n<value>$sparkMasterPublicDNS:8030</value>\n</property>\n<property>\n<name>yarn\.resourcemanager\.address</name>\n<value>$sparkMasterPublicDNS:8050</value>\n</property>' /usr/local/hadoop/etc/hadoop/yarn-site.xml;
cp /usr/local/hadoop/etc/hadoop/mapred-site.xml.template /usr/local/hadoop/etc/hadoop/mapred-site.xml;
sed -i '20 i \n<property>\n<name>mapreduce\.jobtracker\.address</name>\n<value>$sparkMasterPublicDNS:54311</value>\n</property>\n\n<property>\n<name>mapreduce\.framework\.name</name>\n<value>yarn</value>\n\n</property>\n<property>\n<name>mapreduce\.application\.classpath</name>\n<value>/usr/local/hadoop/share/hadoop/mapreduce/*,/usr/local/hadoop/share/hadoop/mapreduce/lib/*,/usr/local/hadoop/share/hadoop/common/*,/usr/local/hadoop/share/hadoop/common/lib/*,/usr/local/hadoop/share/hadoop/tools/lib/*</value>\n</property>' /usr/local/hadoop/etc/hadoop/mapred-site.xml;
sed -i '20 i \n<property>\n<name>dfs\.replication</name>\n<value>3</value>\n</property>\n\n<property>\n<name>dfs\.namenode\.name\.dir</name>\n<value>file:///usr/local/hadoop/hadoop_data/hdfs/namenode</value>\n</property>' /usr/local/hadoop/etc/hadoop/hdfs-site.xml;
echo '$sparkMasterPrivateIp' >> /usr/local/hadoop/etc/hadoop/masters;
sed -i -e 's/localhost/$sparkWorker1PrivateIp/' -e '$ a $sparkWorker2PrivateIp' -e '$ a $sparkWorker3PrivateIp' /usr/local/hadoop/etc/hadoop/slaves;
sudo mkdir -p /usr/local/hadoop/hadoop_data/hdfs/namenode;
sudo chown -R ubuntu /usr/local/hadoop;
sudo mkdir -p /usr/local/hadoop/hadoop_data/hdfs/datanode;
sudo chown -R ubuntu /usr/local/hadoop;
rsync -r /usr/local/hadoop/ ubuntu@$sparkWorker1PrivateIp:/usr/local/hadoop;
rsync -r /usr/local/hadoop/ ubuntu@$sparkWorker2PrivateIp:/usr/local/hadoop;
rsync -r /usr/local/hadoop/ ubuntu@$sparkWorker3PrivateIp:/usr/local/hadoop;
cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh;
sed -i -e 's/# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname/export SPARK_MASTER_HOST=$sparkMasterPrivateIp/' -e '$ a export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' -e '$ a export PYSPARK_PYTHON=python3' /usr/local/spark/conf/spark-env.sh;
cp /usr/local/spark/conf/slaves.template /usr/local/spark/conf/slaves;
sed -i -e 's/localhost/$sparkWorker1PrivateIp/' -e '$ a $sparkWorker2PrivateIp' -e '$ a $sparkWorker3PrivateIp' /usr/local/spark/conf/slaves;
cp /usr/local/spark/conf/spark-defaults.conf.template /usr/local/spark/conf/spark-defaults.conf;
sed -i -e '$ a spark.jars = /usr/local/spark/jars/hadoop-aws-2.7.7.jar, /usr/local/spark/jars/aws-java-sdk-1.7.4.jar, /usr/local/spark/jars/spark-avro_2.11-4.0.0.jar' /usr/local/spark/conf/spark-defaults.conf;
exit
"

echo "spark master IP address:\t$sparkMasterPublicIp"
echo "worker 1 IP address:\t$sparkWorker1PublicIp"
echo "worker 2 IP address:\t$sparkWorker2PublicIp"
echo "worker 3 IP address:\t$sparkWorker3PublicIp"


# spark master IP address:	54.202.33.83
# worker 1 IP address:	52.34.108.191
# worker 2 IP address:	54.200.159.139
# worker 3 IP address:	52.34.250.123


##	Next steps:
#	log back in
# ssh -i $HOME/.ssh/JLevitt-IAM-keypair.pem ubuntu@$sparkMasterPublicIp
# hdfs namenode -format;
# sh /usr/local/hadoop/sbin/start-dfs.sh;
# sh /usr/local/hadoop/sbin/start-yarn.sh;
# sh /usr/local/spark/sbin/start-all.sh


##	Python Note: 
#	sudo apt-get install -y python3-pip
#	export PYTHONPATH=/usr/lib/python3.5		##Check version
#	export PYSPARK_PYTHON=/usr/bin/python3.5	##Check version