# sqsmove :postal_horn:

Interactive script that allows you to send messages from one queue to another in sqs.

## Setup

Create a .aws folder with two files, "config" and "credentials" and enter your access information and config as explained [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html).

**credentials**
```
[default]
aws_access_key_id=XXXX
aws_secret_access_key=XXXXX
```

**config**
```
[default]
region=<<your aws region>>
output=json
```

## Run

```
git clone git@github.com:okarlsson/sqsmove.git
cd sqsmove
npm install
node index
```

## Options

Filter messages
```
node index -f string / --filter string
```

Concurrency - how many requests to sqs to run concurrently
```
node index -c 15 / --concurrency 15
```

## Todos

1. Give an option to delete the messages from the old queue
2. Use the sendMessageBatch() method to send messages in batches instead of one at the time