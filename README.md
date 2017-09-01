# AWS-Lambda_EBS-snapshots
AWS Lambda triggering EBS snapshots
Backup deletion code will be updated soon



## Setting Up IAM Permissions

First create an IAM policy called "lambda-ebs_policy" with the following policy document:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:*"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": "ec2:Describe*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateSnapshot",
                "ec2:DeleteSnapshot",
                "ec2:CreateTags",
                "ec2:ModifySnapshotAttribute",
                "ec2:ResetSnapshotAttribute"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sns:Publish"
            ],
            "Resource": "*"
        }
    ]
}
```

Next create an IAM role also called "lambda-ebs_role" select "AWS Lambda" as the Role type, then attach the "lambda-ebs_policy" policy created above.


## Add the SNS Topics ARN you want publish as a Lambda environment variable "aws_sns_arn"

This is optional environment variable if you want publish any topic, so you might receive email notification
once EBS backing up was executed.

## Create the Lambda Functions

Create a functions in Lambda using the Python 2.7 runtime. I recommend just using the 128 MB memory setting, and adjust the timeout to 10 seconds (longer in a larger environment). Set the event source to be "CloudWatch Events - Schedule" and set the Schedule expression to be a cron expression of your liking i.e. "cron(0 6 * * ? *)" if you want the job to be kicked off at 06:00 UTC.

## Tagging your EC2 instances to backup

You will need to tag your instances in order for them to be backed up, below are the tags that will be used by the Lambda function:

| Tag Key           | Tag Value                           | Notes |
| -------------     |:-------------:                      | -----:|
|Backup             |                                     | Value Not Needed |
|Retention          | *Number of Days to Retain Snapshot* | Default is 7 Days| 
|Skip_Backup_Volumes| volume id(s) in CSV string          | List either a single volume-id, or multiple volumes-ids in a Comma Separated Value String|
## More Info


[if you need more details on setting this up please mail me to rajeshkrish12@gmail.com] 
