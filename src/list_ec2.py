"""Example AWS script to list EC2 instances."""

from aws_utils import get_aws_session

def list_ec2_instances(region=None):
    """
    List all EC2 instances in the specified region.
    
    Args:
        region (str, optional): AWS region. Defaults to None.
    
    Returns:
        list: List of EC2 instance information
    """
    session = get_aws_session()
    ec2 = session.client('ec2', region_name=region)
    
    response = ec2.describe_instances()
    instances = []
    
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instances.append({
                'InstanceId': instance['InstanceId'],
                'InstanceType': instance['InstanceType'],
                'State': instance['State']['Name']
            })
    
    return instances

if __name__ == '__main__':
    instances = list_ec2_instances()
    print("EC2 Instances:")
    for instance in instances:
        print(f"ID: {instance['InstanceId']}")
        print(f"Type: {instance['InstanceType']}")
        print(f"State: {instance['State']}")
        print("-" * 30)