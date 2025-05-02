import { SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

// Create DynamoDB client
const client = createDDbDocClient();
const tableName = process.env.TABLE_NAME!;

export const handler: SQSHandler = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    // Process each message from SQS (Queue B)
    for (const record of event.Records) {
      const message = JSON.parse(record.body);
      console.log("Processing message:", JSON.stringify(message));
      
      // Part C requirement: Only process messages without an email property
      // Check if the message has an email property
      if (message.email) {
        console.log("Message has email property, skipping:", message.email);
        continue; // Skip this message
      }
      
      // Make sure the message has the required country property (Ireland or China)
      if (!message.address || !message.address.country || 
          (message.address.country !== 'Ireland' && message.address.country !== 'China')) {
        console.log("Message doesn't have required country (Ireland or China), skipping");
        continue;
      }
      
      console.log("Processing message without email from country:", message.address.country);
      
      // Store the message in DynamoDB
      const timestamp = new Date().toISOString();
      await client.send(
        new PutCommand({
          TableName: tableName,
          Item: {
            movieId: Date.now(), // Using timestamp as movieId for demo
            role: `no-email-${timestamp}`,
            name: message.name || "Unknown",
            address: message.address,
            source: "lambda-y",
            timestamp
          }
        })
      );
      
      console.log("Successfully stored message without email in DynamoDB");
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
