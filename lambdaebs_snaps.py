import boto3
import collections
import datetime
import base64
import os
import sys
import json
import itertools

ec = boto3.client('ec2')
#base64_region = os.environ['aws_regions']
aws_sns_arn = os.getenv('aws_sns_arn', None)

def send_to_sns(subject, message):
    if aws_sns_arn is None:
        return

    print "Sending notification to: %s" % aws_sns_arn

    client = boto3.client('sns')

    response = client.publish(
        TargetArn=aws_sns_arn,
        Message=message,
        Subject=subject)

    if 'MessageId' in response:
        print "Notification sent with message id: %s" % response['MessageId']
    else:
        print "Sending notification failed with response: %s" % str(response)

def lambda_handler(event, context):

        reservations = ec.describe_instances(
            Filters=[
                {'Name': 'tag-key', 'Values': ['backup', 'Backup']},
            ]
        ).get(
            'Reservations', []
        )

        instances = sum(
            [
                [i for i in r['Instances']]
                for r in reservations
            ], [])

        print "Found %d instances that need backing up " % (len(instances))

        to_tag_retention = collections.defaultdict(list)
        to_tag_mount_point = collections.defaultdict(list)

        for instance in instances:
            try:
                ins_name = [
                    str(t.get('Value')).split(',')for t in instance['Tags']
                    if t['Key'] == 'Name'][0]
            except Exception:
                pass
            try:
                retention_days = [
                    int(t.get('Value')) for t in instance['Tags']
                    if t['Key'] == 'Retention'][0]
            except IndexError:
                retention_days = 7

            try:
                skip_volumes = [
                    str(t.get('Value')).split(',') for t in instance['Tags']
                    if t['Key'] == 'Skip_Backup_Volumes']
            except Exception:
                pass

            from itertools import chain
            skip_volumes_list = list(chain.from_iterable(skip_volumes))

            for dev in instance['BlockDeviceMappings']:
                if dev.get('Ebs', None) is None:
                    continue
                vol_id = dev['Ebs']['VolumeId']
                if vol_id in skip_volumes_list:
                    print "Volume %s is set to be skipped, not backing up" % (vol_id)
                    continue
                dev_attachment = str(ins_name).strip('[]'"'") + '-' + dev['DeviceName']
                print "Found EBS volume %s on instance %s attached to %s" % (
                    vol_id, instance['InstanceId'], dev_attachment)

                snap = ec.create_snapshot(
                    VolumeId=vol_id,
                    Description="Lambda - " + instance['InstanceId'],
                )

                to_tag_retention[retention_days].append(snap['SnapshotId'])
                to_tag_mount_point[vol_id].append(snap['SnapshotId'])
                

                print "Retaining snapshot %s of volume %s from instance %s for %d days" % (
                    snap['SnapshotId'],
                    vol_id,
                    instance['InstanceId'],
                    retention_days,
                )

                ec.create_tags(
                    Resources=to_tag_mount_point[vol_id],
                    Tags=[
                        {'Key': 'Name', 'Value': dev_attachment},
                    ]
                )
        

        for retention_days in to_tag_retention.keys():
            delete_date = datetime.date.today() + datetime.timedelta(days=retention_days)
            delete_fmt = delete_date.strftime('%Y-%m-%d')
            print "Will delete %d snapshots on %s" % (len(to_tag_retention[retention_days]), delete_fmt)
            ec.create_tags(
                Resources=to_tag_retention[retention_days],
                Tags=[
                    {'Key': 'DeleteOn', 'Value': delete_fmt},
                ]
            )

        message = "{} instances volume have been backed up".format(len(instances))
        send_to_sns('EBS Backups', message)
