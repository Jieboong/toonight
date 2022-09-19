ssh -i C:/Users/syahn/Downloads/projects/Toonight/toonightEC2키페어/toonight_aws_key.pem ec2-user@3.36.162.170

flintrock launch spark
flintrock add-slaves spark --num-slaves 1
flintrock login spark

yum -y install python3

pip3 install pyspark

echo "export AWS_ACCESS_KEY_ID=ACCESS_KEY" >> ~/.bashrc
echo "export AWS_SECRET_ACCESS_KEY=SECRET_KEY" >> ~/.bashrc
source ~/.bashrc


cd spark/jars


wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.563/aws-java-sdk-core-1.11.563.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.563/aws-java-sdk-s3-1.11.563.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.11.563/aws-java-sdk-dynamodb-1.11.563.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.563/aws-java-sdk-1.11.563.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar



cd
pip3 install pandas
aws s3 cp s3://toonightbucket/sparkConf/webtoon_process.py ./
aws s3 cp s3://toonightbucket/sparkConf/Topicmodeling.py ./
aws s3 cp s3://toonightbucket/sparkConf/fin_process.py ./
aws s3 cp s3://toonightbucket/sparkConf/comment_process.py ./

pip3 install boto3
pip3 install gensim
pip3 install konlpy
sudo yum install git


wget https://bitbucket.org/eunjeon/mecab-ko/downloads/mecab-0.996-ko-0.9.2.tar.gz
tar xvfz mecab-0.996-ko-0.9.2.tar.gz
cd mecab-0.996-ko-0.9.2
./configure

make
make check
make install
sudo ldconfig

mecab --version

cd
wget https://bitbucket.org/eunjeon/mecab-ko-dic/downloads/mecab-ko-dic-2.1.1-20180720.tar.gz
tar xvfz mecab-ko-dic-2.1.1-20180720.tar.gz
cd mecab-ko-dic-2.1.1-20180720
./configure
make
make install


cd
pip3 install sklearn
spark-submit webtoon_process.py
spark-submit fin_process.py
spark-submit comment_process.py
spark-submit Topicmodeling.py

