import { SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

// Create DynamoDB client
const client = createDDbDocClient();
const tableName = process.env.TABLE_NAME!;

export const handler: SQSHandler = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    // Process each message from SQS
    for (const record of event.Records) {
      const message = JSON.parse(record.body);
      
      // If message is from SNS, unwrap it
      const payload = message.Message ? JSON.parse(message.Message) : message;
      
      console.log("Processing message:", JSON.stringify(payload));
      
      // Store the event data in DynamoDB
      const timestamp = new Date().toISOString();
      
      await client.send(
        new PutCommand({
          TableName: tableName,
          Item: {
            movieId: Date.now(), // Using timestamp as movieId for demo
            role: `event-${timestamp}`, // Using timestamp for sort key
            event: payload,
            source: "lambda-x",
            timestamp
          }
        })
      );
      
      console.log("Successfully stored event in DynamoDB");
    }
    
    console.log("Successfully processed all messages");
  } catch (error: any) {
    console.error("Error:", error);
    throw new Error(JSON.stringify(error));
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
