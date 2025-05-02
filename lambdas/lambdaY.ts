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
      let message;
      try {
        message = JSON.parse(record.body);
        
        // If the message is from SNS, it may be wrapped in an SNS envelope
        if (message.Message) {
          try {
            message = JSON.parse(message.Message);
          } catch (err) {
            // If parsing fails, use the Message as is
            message = message.Message;
          }
        }
        
        console.log("Processing message:", JSON.stringify(message));
        
        // PART C: Filter messages - Only process those from Ireland or China that DON'T have an email
        const country = message.address?.country;
        const hasEmail = !!message.email;
        
        // Check if the message meets the filter criteria
        if (!country || (country !== 'Ireland' && country !== 'China')) {
          console.log(`Skipping message: country ${country} is not Ireland or China`);
          continue;
        }
        
        // Part C: Handle messages WITHOUT an email property
        if (hasEmail) {
          console.log("Skipping message: has email property", message.email);
          continue;
        }
        
        console.log(`Processing message from ${country} without email`);
        
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
      } catch (err) {
        console.error("Error processing message:", err);
        // Continue processing other messages even if one fails
      }
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
