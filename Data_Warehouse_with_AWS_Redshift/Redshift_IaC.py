import configparser
import logging
import logging.config
from pathlib import Path
import argparse
import boto3
import time
import json
from botocore.exceptions import ClientError


# Initializing logger to logg all script actions
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.cfg")
logger = logging.getLogger(__name__)

# Loading configurations for cluster from config file
config = configparser.ConfigParser()
config.read_file(open('cluster.cfg'))

def boolean_parser(value):
    if value.upper() not in ['FALSE', 'TRUE']:
        logging.error(f"Invalid arguemnt : {value}. Must be TRUE or FALSE")
        raise ValueError('Not a valid boolean string')
    return value.upper() == 'TRUE'


def create_role_IAM(iam_service):
    """
    Create IAM role with configurations defined in cluster.cfg
    :param iam_service: IAM service instance
    :return: True with successful creation of IAM role
    """
    
    iam_role_name = config.get('IAM_ROLE', 'IAM_ROLE_NAME')
    iam_role_description = config.get('IAM_ROLE', 'DESCRIPTION')
    iam_role_policy_arn = config.get('IAM_ROLE','POLICY_ARN')
    
    logging.info(f"Creating IAM role name : {iam_role_name}, description : {iam_role_description} and policy : {iam_role_policy_arn}")
    
    role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "Service": [ "redshift.amazonaws.com" ] },
                    "Action": [ "sts:AssumeRole" ]
                    
                }
            ]
        }
    )
    
    try:
        create_response = iam_service.create_role(
                    Path='/',
                    RoleName=iam_role_name,
                    Description=iam_role_description,
                    AssumeRolePolicyDocument = role_policy_document
        )
        logger.debug(f"Recieved response from IAM client following creation of IAM role : {create_response}")
        logger.info(f"IAM Role creation response code : {create_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error while creating iam role : {e}")
        return False
    
    try:
        # Attaching role policy using Amazon resource names
        policy_response = iam_service.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=iam_role_policy_arn
        )
        logger.debug(f"Recieved response from IAM client for applying policy to role : {policy_response}")
        logger.info(f"Attach policy response code : {policy_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occured while applying policy : {e}")
        return False

    return True if( (create_response['ResponseMetadata']['HTTPStatusCode'] == 200) and  (policy_response['ResponseMetadata']['HTTPStatusCode'] == 200) ) else False

def retrieve_group(ec2_client, group_name):
    groups = \
    ec2_client.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [group_name]}])[
        'SecurityGroups']
    return None if(len(groups) == 0) else groups[0]

def create_security_group_ec2(ec2_service):

    if(retrieve_group(ec2_service, config.get('SECURITY_GROUP','NAME')) is not None):
        logger.info("Group already exists!!")
        return True

    # Fetch VPC ID
    vpc_id = ec2_service.describe_security_groups()['SecurityGroups'][0]['VpcId']

    response = ec2_service.create_security_group(
        Description=config.get('SECURITY_GROUP','DESCRIPTION'),
        GroupName=config.get('SECURITY_GROUP','NAME'),
        VpcId=vpc_id,
        DryRun=False
    )
    logger.debug(f"Response recieved following creation of security group: {response}")
    logger.info(f"Security Group created. Response code: {response['ResponseMetadata']['HTTPStatusCode']}")

    logger.info("Authorizing security group ingress")
    ec2_service.authorize_security_group_ingress(
                GroupId=response['GroupId'],
                GroupName=config.get('SECURITY_GROUP','NAME'),
                FromPort=int(config.get('INBOUND_RULE','PORT_RANGE')),
                ToPort=int(config.get('INBOUND_RULE', 'PORT_RANGE')),
                CidrIp=config.get('INBOUND_RULE','CIDRIP'),
                IpProtocol=config.get('INBOUND_RULE','PROTOCOL'),
                DryRun=False
    )

    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)


def create_redshift_cluster(redshift_service, iam_role, vpc_security_group_id):
    """
    SPIN UP a Redshift cluster using the IAM role and security group created.
    :param redshift_client: a redshift service instance
    :param iam_role: IAM role arn to give permission to cluster to communicate with other AWS service
    :param vpc_security_group_id: vpc group for network setting for cluster
    :return: True if cluster created successfully.
    """

    # Setting cluster configurations config
    cluster_type = config.get('CLUSTER','CLUSTER_TYPE')
    node_type =  config.get('CLUSTER', 'NODE_TYPE')
    num_nodes = int(config.get('CLUSTER', 'NUM_NODES'))

    # Cluster identifiers and credentials
    cluster_identifier = config.get('CLUSTER','CLUSTER_IDENTIFIER')
    db_name = config.get('CLUSTER', 'DB_NAME')
    database_port=int(config.get('CLUSTER','DB_PORT'))
    master_username = config.get('CLUSTER', 'DB_USER')
    master_user_password = config.get('CLUSTER', 'DB_PASSWORD')

    # Security settings
    security_group = config.get('SECURITY_GROUP', 'NAME')

    
    try:
        response = redshift_service.create_cluster(
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            VpcSecurityGroupIds=vpc_security_group_id,
            IamRoles = [iam_role]
        )
        logger.debug(f"Recieved Cluster creation response : {response}")
        logger.info(f"Recieved Cluster creation response code : {response['ResponseMetadata']['HTTPStatusCode']} ")
    except Exception as e:
        logger.error(f"Error: Exception occured while creating cluster : {e}")
        return False
    
    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)

def get_cluster_status(redshift_client, cluster_identifier):

    response = redshift_client.describe_clusters(ClusterIdentifier = cluster_identifier)
    cluster_status = response['Clusters'][0]['ClusterStatus']
    logger.info(f"Cluster status : {cluster_status.upper()}")
    return True if(cluster_status.upper() in ('AVAILABLE','ACTIVE', 'INCOMPATIBLE_NETWORK', 'INCOMPATIBLE_HSM', 'INCOMPATIBLE_RESTORE', 'INSUFFICIENT_CAPACITY', 'HARDWARE_FAILURE')) else False

def delete_cluster(redshift_service):
    """
    Deleting the redshift cluster
    :param redshift_client: a redshift client instance
    :return: True if cluster deleted successfully.
    """

    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    if(len(redshift_service.describe_clusters()['Clusters']) == 0):
        logger.info(f"Cluster {cluster_identifier} does not exist.")
        return True

    try:
        while(not get_cluster_status(redshift_service, cluster_identifier=cluster_identifier)):
            logger.info("Can't delete cluster. Waiting for cluster to become ACTIVE")
            time.sleep(10)
        response = \
            redshift_service.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)
        logger.debug(f"Cluster deleted with response : {response}")
        logger.info(f"Cluster deleted response code : {response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Exception occured while deleting cluster : {e}")
        return False

    return response['ResponseMetadata']['HTTPStatusCode']


def delete_ec2_security_group(ec2_service):
    """
    Delete a security group
    :param ec2_client: ec2 client instance
    :return: True if security group deleted successfully
    """

    group_name = config.get('SECURITY_GROUP','NAME')
    group = retrieve_group(ec2_service, group_name)

    if(group is None):
        logger.info(f"Group {group_name} does not exist")
        return True

    try:
        response = ec2_service.delete_security_group(
            GroupId=group['GroupId'],
            GroupName=group_name,
            DryRun=False
        )
        logger.debug(f"Deleting security group response : {response}")
        logger.info(f"Delete response {response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occured while deleting group : {e}")
        return False

    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)


def delete_IAM_role(iam_client):
    """
    Delete and IAM role
    Make sure that you do not have any Amazon EC2 instances running with the role you are about to delete.
    Deleting a role or instance profile that is associated with a running instance will break any applications running on the instance.
    :param iam_client: an IAM service client instance
    :return: True if role deleted successfully.
    """

    role_name = config.get('IAM_ROLE', 'IAM_ROLE_NAME')

    existing_roles = [role['RoleName'] for role in iam_client.list_roles()['Roles']]
    if(role_name not in existing_roles):
        logger.info(f"Role {role_name} does not exist.")
        return True

    logger.info(f"Processing deleting IAM role : {role_name}")
    try:
        detach_response = iam_client.detach_role_policy(RoleName=role_name, PolicyArn=config.get('IAM_ROLE','POLICY_ARN'))
        logger.debug(f"Response for policy detach from IAM role : {detach_response}")
        logger.info(f"Detach policy response code : {detach_response['ResponseMetadata']['HTTPStatusCode']}")
        delete_response = iam_client.delete_role(RoleName=role_name)
        logger.debug(f"Response for deleting IAM role : {delete_response}")
        logger.info(f"Delete role response code : {delete_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Exception occured while deleting role : {e}")
        return False

    return True if( (detach_response['ResponseMetadata']['HTTPStatusCode'] == 200) and  (delete_response['ResponseMetadata']['HTTPStatusCode'] == 200) ) else False


if __name__ == "__main__":
    
    # Setting up all script arguments
    parser = argparse.ArgumentParser(description="Redshift cluster infrastructure as code. Creates IAM role with security group and spins up Redshift cluster")
    
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument("-c", "--create", type=boolean_parser, metavar='', required=True,
                          help="True or False. Create IAM roles, security group and redshift cluster")
    required.add_argument("-d", "--delete", type=boolean_parser, metavar='', required=True,
                          help="True or False. Delete the roles, securitygroup and cluster. CAUTION: Deletes the Redshift cluster, IAM role and security group. ")
    optional.add_argument("-v", "--verbosity", type=boolean_parser, metavar='', required=False, default=True,
                          help="Increase output verbosity. Default set for more deverse output")
    args = parser.parse_args()
    logger.info(f"ARGS : {args}")

    if(not args.verbosity):
        logger.setLevel(logging.INFO)
        logger.info("LOGGING LEVEL SET TO INFO.")
        
    # Creating services for IAM, EC2, S3 and Redshift
    ec2 = boto3.client(service_name = 'ec2', region_name = 'us-east-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))

    s3 = boto3.client(service_name = 's3', region_name = 'us-east-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))

    iam = boto3.client(service_name = 'iam', region_name = 'us-east-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))

    redshift = boto3.client(service_name = 'redshift', region_name = 'us-east-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))

    logger.info("Setup all client services successfully.")
    
    # Creating IAM role, adding security group and spinning up Amazon Redshift cluster
    if(args.create):
        if(create_role_IAM(iam)):
            logger.info("Succesfully created IAM role. Initializing security group for IAM role ..")
            if(create_security_group_ec2(ec2)):
                logger.info("Security group created. Spinning up a AWS redshift cluster....")
                role_arn = iam.get_role(RoleName = config.get('IAM_ROLE', 'IAM_ROLE_NAME'))['Role']['Arn']
                vpc_security_group_id = retrieve_group(ec2, config.get('SECURITY_GROUP', 'NAME'))['GroupId']
                create_redshift_cluster(redshift, role_arn, [vpc_security_group_id])
            else:
                logger.error("Error failed to create security group")
        else:
            logger.error("Error failed to create IAM role")
    else:
        logger.info("Skipping Creation.")
        
        
        
    # deleting all unnecessary services
    if(args.delete):
        delete_cluster(redshift)
        delete_ec2_security_group(ec2)
        delete_IAM_role(iam)