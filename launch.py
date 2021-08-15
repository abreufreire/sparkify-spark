#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
from botocore.exceptions import ClientError
import configparser


def create_client(REGION, AWS_KEY, AWS_SECRET):
    """
    creates clients for EC2, S3, IAM (Identify & Access Management) & Redshift.
    :param REGION: config parameter
    :param AWS_KEY: config parameter
    :param AWS_SECRET: config parameter
    :return: s3, emr (client for ) objects
    """

    print("\ncreating aws clients...")
    s3 = boto3.client("s3",
                      region_name=REGION,
                      aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET
                      )

    emr = boto3.client("emr",
                       region_name=REGION,
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )
    '''
    notes: 
    boto3.client (original api abstraction: makes low-level calls) vs boto3.resource (makes high-level calls)
    '''

    return s3, emr


def create_emr_cluster(emr, config):

    cluster_id = emr.run_job_flow(
        Name="emr-cluster",
        LogUri="s3://aws-logs-758818281029-us-west-2",
        ReleaseLabel="emr-5.33.0",
        Applications=[
            {
                "Name": "Spark"
            }
        ],

        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties":
                            {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                    }
                ]
            }
        ],

        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                },

                {
                    "Name": "Slave nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 4
                }
            ],

            "Ec2KeyName": config.get("EC2", "keys"),

            "KeepJobFlowAliveWhenNoSteps": False,  # shutdown

            "TerminationProtected": False,  # cluster is unlocked to be terminated

            "Ec2SubnetId": config.get("EC2", "subnet")
        },

        Steps=[
            {
                "Name": "setup-debug-step",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep":
                    {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "state-pusher-script"
                        ]
                    }
            },

            {
                "Name": "setup-copy-step",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep":
                    {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "aws",
                            "s3",
                            "cp",
                            "s3://" + config.get("S3", "script_bucket"),
                            "/home/hadoop/",
                            "--recursive"
                        ]
                    }
            },

            {
                "Name": "run-spark-step",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep":
                    {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "/home/hadoop/etl.py",
                            config["S3"]["input_data"],
                            config["S3"]["output_data"]
                        ]
                    }
                }
        ],

        VisibleToAllUsers=True,

        JobFlowRole="EMR_EC2_DefaultRole",

        ServiceRole="EMR_DefaultRole"
    )

    print("\ncluster created - ID: {}".format(cluster_id["JobFlowId"]))


def create_bucket(s3, bucket_name, region):
    try:
        create_bucket_response = s3.create_bucket(Bucket=bucket_name,
                                                  CreateBucketConfiguration={'LocationConstraint': region})

    except ClientError as err:
        print("\nexception in create bucket: {}".format(err))
        # sys.exit(1)


def upload_etl_to_s3(s3, script, bucket_name):
    try:
        s3.upload_file(script, bucket_name, "etl.py")
    except ClientError as err:
        print("\nexception with upload file: {}".format(err))
        # sys.exit(1)


def launch():
    """
    creates AWS clients;
    launches EMR cluster (terminates after finish jobs);
    creates S3 buckets;
    uploads code/script to S3 bucket.
    """

    # gets parameters from config file dl.cfg
    config = configparser.ConfigParser()
    config.read_file(open("dl.cfg"))

    # pair of individual access keys
    AWS_KEY = config.get("default", "aws_access_key_id")
    AWS_SECRET = config.get("default", "aws_secret_access_key")

    # config dl parameters
    REGION = config.get("S3", "region")
    SCRIPT_BUCKET = config.get("S3", "script_bucket")
    OUTPUT_BUCKET = config.get("S3", "output_bucket")

    # FUNCTIONS
    s3, emr = create_client(REGION, AWS_KEY, AWS_SECRET)

    create_bucket(s3, OUTPUT_BUCKET, REGION)

    create_bucket(s3, SCRIPT_BUCKET, REGION)

    script_path = "etl.py"
    upload_etl_to_s3(s3, script_path, SCRIPT_BUCKET)

    create_emr_cluster(emr, config)


if __name__ == '__main__':
    launch()
