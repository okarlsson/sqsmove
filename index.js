"use strict";

const stdio = require("stdio");
const AWS = require("aws-sdk");
const inquirer = require("inquirer");
const pAll = require("p-all");
const pDoWhilst = require("p-do-whilst");

const options = stdio.getopt(
  {
    concurrency: {
      args: 1,
      description: 'The level of concurrency used to process the messages',
      key: 'c',
      default: 10
    },
    FILTER: {
      args: 1,
      description: 'The text string that my message needs to contain',
      key: 'f',
      default: null
    }
  }
);

const sqs = new AWS.SQS({region: "eu-west-1"});
const concurrency = options.concurrency;

async function run() {
  var data = await listQueues().catch(err => {
    console.log(err);
    process.exit(1);
  });

  let queues = [];

  for (var i = 0; i < data.QueueUrls.length; i++) {
    var tagsResponse = await getTags(data.QueueUrls[i]);

    var value = data.QueueUrls[i];
    var tags = tagsResponse.Tags;
    var name = `url: ${value}, tags: ${tags ? JSON.stringify(tags) : "none"}`;

    queues.push({ name: name, value: value });
  }

  var response = await selectQueues(queues);
  var messages = await getMessages(response.fromQueue);
  console.log(`\nRead ${messages.length} messages from queue`);

  if (messages.length == 0) {
    console.log("No messages to move!");
    process.exit(0);
  }

  var parsedMessages = parseMessages(messages);

  if (options.FILTER) {
    parsedMessages = parsedMessages.filter(message =>
      filterMessage(message, options.FILTER)
    );
    console.log(
      `Filtered out ${parsedMessages.length} for ${options.FILTER}`
    );
  }

  await sendMessages(response.toQueue, parsedMessages);
  console.log("\nAll messages sent!");
}

function parseMessages(messages) {
  return messages.map(message => JSON.parse(message.Body));
}

function filterMessage(message, filter) {
  return JSON.stringify(message).includes(filter);
}

async function sendMessages(url, messages) {
  var totalCount = messages.length;
  var count = 0;

  return new Promise(resolve => {
    console.log(`Sending ${messages.length} messages to ${url}`);

    pDoWhilst(
      () => {
        var countToExecute =
          messages.length > concurrency ? concurrency : messages.length;
        const actions = [];

        for (var i = 0; i < countToExecute; i++) {
          actions.push(() => sqsSendMessage(url, messages.pop()));
        }

        return pAll(actions, { concurrency }).then(results => {
          count += results.length;

          process.stdout.clearLine();
          process.stdout.cursorTo(0);
          process.stdout.write(`Messages sent.. ${count}/${totalCount}`);
        });
      },
      () => {
        return messages.length > 0;
      }
    ).then(() => resolve());
  });
}

async function getMessages(url) {
  var totalCount = await getNumMessagesInQueue(url);
  var visibilityTimeout = calculateTimeout(totalCount);
  var count = 0;
  var messages = [];

  console.log("Total count: " + totalCount);

  return new Promise(resolve => {
    pDoWhilst(
      () => {
        const actions = [];

        for (var i = 0; i < concurrency; i++) {
          actions.push(() => sqsRecieveMessage(url, visibilityTimeout));
        }

        return pAll(actions, { concurrency: concurrency }).then(results => {
          for (var i = 0; i < results.length; i++) {
            var result = results[i];

            if (!result.Messages || result.Messages.length == 0) {
              continue;
            }

            count += result.Messages.length;

            messages.push(...result.Messages);
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            process.stdout.write(`Reading messages.. ${count} / ${totalCount}`);
          }
        });
      },
      () => {
        return count < totalCount;
      }
    ).then(() => resolve(messages));
  });
}

function getNumMessagesInQueue(url) {
  var params = {
    QueueUrl: url /* required */,
    AttributeNames: ["ApproximateNumberOfMessages"]
  };

  return new Promise((resolve, reject) => {
    sqs.getQueueAttributes(params, function(err, data) {
      if (err) reject(err);
      else resolve(data.Attributes.ApproximateNumberOfMessages);
    });
  });
}

function calculateTimeout(totalCount) {
  var timeout = totalCount / 100;
  return timeout < 60 ? 60 : timeout;
}

function sqsSendMessage(url, message) {
  var params = {
    MessageBody: JSON.stringify(message),
    QueueUrl: url /* required */,
    DelaySeconds: 0
  };

  return new Promise((resolve, reject) => {
    sqs.sendMessage(params, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

function sqsRecieveMessage(url, timeout) {
  var params = {
    QueueUrl: url /* required */,
    AttributeNames: ["ALL"],
    MaxNumberOfMessages: 10 /* Max number of messages that sqs allows */,
    WaitTimeSeconds: 0,
    VisibilityTimeout: timeout
  };

  return new Promise((resolve, reject) => {
    sqs.receiveMessage(params, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

function selectQueues(queues) {
  const questions = [
    {
      type: "list",
      name: "fromQueue",
      message: "Choose the queue you want to move messages from",
      choices: [...queues]
    },
    {
      type: "list",
      name: "toQueue",
      message: "Choose the queue you want to move messages to",
      choices: [...queues]
    }
  ];

  return new Promise(resolve => {
    inquirer.prompt(questions).then(answers => {
      if (answers.fromQueue === answers.toQueue) {
        console.log("From and to queues must be different");
        process.exit(1);
      } else {
        resolve(answers);
      }
    });
  });
}

async function getTags(queueUrl) {
  return new Promise((resolve, reject) => {
    var params = {
      QueueUrl: queueUrl
    };

    sqs.listQueueTags(params, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

async function listQueues() {
  return new Promise((resolve, reject) => {
    sqs.listQueues(null, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

run();
