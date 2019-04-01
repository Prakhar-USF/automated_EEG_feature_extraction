### Enter your aws access and secret key  in "aws_access_key_id" and "aws_secret_access_key"
### BootstrapActions fetches features.py from test.sh at s3 bucket s3://usfca-msds694/eeg, 
### creates a folder contents in /home/hadoop and downloads it.
### Enter your key-pair name in "Ec2KeyName"

import boto3


connection = boto3.client(
    'emr',
    region_name='us-west-2',
    aws_access_key_id='ENTER_YOUR_KEY',
    aws_secret_access_key='ENTER_YOUR_ACCESS',
)

cluster_id = connection.run_job_flow(
    Name='test_emr_job_with_boto3',
    ReleaseLabel='emr-5.20.0',
    CustomAmiId = 'ami-0c4922abab3466671',
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'c5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'c5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'Ec2KeyName': 'DBV',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    Steps=[],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    EbsRootVolumeSize = 50 ,
    BootstrapActions=[
            {"Name": "features.py",
             "ScriptBootstrapAction": {
                 "Path": "s3://usfca-msds694/eeg/test.sh" }} 
            ]
    ,
    Configurations = [
   {
"Classification":"spark-env",
        "Properties":{},
        "Configurations":[
            {"Classification":"export",
            "Properties":{"PYSPARK_PYTHON":"/opt/anaconda3/binpython"}
                }
                ]
}
]
)

print (cluster_id['JobFlowId'])