# Automated EBS anpshot cleanup
# @author Rajesh krishnamoorthy rajeshkrish12@gmail.com
# This script will search for all snapshots having a tag with "DeleteOn" or "deleteon"
# on it. As soon as we have the snapshot list, we loop through each instance
# and get the vlaue of delete on tag.then the value is compared with this day's date and 
# deletes the snapshot if the comapared date is lesser than today
# an email is triggered to the given sns arnwith the count of the deleted snapshots 
# same IAM permission used for backup can be used here 
# Refer my repo for IAM permissions and much more https://github.com/rajeshkrish12/AWS-Lambda_EBS-snapshots
import boto3
import re
import datetime
import time
import base64
import os
import json
import collections
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
  ec = boto3.client('ec2')
  myAccount = boto3.client('sts').get_caller_identity()['Account']
  to_tag = collections.defaultdict(list)

  filter = [
            {'Name': 'tag-key', 'Values': ['DeleteOn', 'deleteon']},
        ]

  snapshots = ec.describe_snapshots(MaxResults=1000, OwnerIds=[myAccount],Filters=filter)['Snapshots']
  for snapshot in snapshots:
         try:
            deletion_date = [
              t.get('Value') for t in snapshot['Tags']
              if t['Key'] == 'DeleteOn'][0]
            delete_date = time.strptime(deletion_date, "%Y-%m-%d")
  
         except IndexError:
            delete_date = False
            deletion_date = False
        # for snap in snapshot:    
         print delete_date

         date_today = datetime.date.today().strftime('%Y-%m-%d')  
          
      
         today_time = datetime.datetime.now().strftime('%Y-%m-%d')
                # today_fmt = today_time.strftime('%m-%d-%Y')
         today_date = time.strptime(today_time, '%Y-%m-%d')

                # If image's DeleteOn date is less than or equal to today,
                # add this image to our list of images to process later
         if delete_date <= today_date:
            print snapshot['SnapshotId']
            print "Deleting snapshot %s" % snapshot['SnapshotId']
            ec.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
  message = "Hello,\n \nEBS cleanup has been initiated successfully for {} instances".format(len(snapshots))
  send_to_sns('EBS Cleanup', message)
