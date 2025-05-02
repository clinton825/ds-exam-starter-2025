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
        
        // PART B: Filter messages - Only process those from Ireland or China that have an email
        const country = message.address?.country;
        const hasEmail = !!message.email;
        
        // Check if the message meets the filter criteria
        if (!country || (country !== 'Ireland' && country !== 'China')) {
          console.log(`Skipping message: country ${country} is not Ireland or China`);
          continue;
        }
        
        if (!hasEmail) {
          console.log("Skipping message: missing email property");
          continue;
        }
        
        console.log(`Processing message from ${country} with email ${message.email}`);
        
        // Store the event data in DynamoDB
        const timestamp = new Date().toISOString();
        
        await client.send(
          new PutCommand({
            TableName: tableName,
            Item: {
              movieId: Date.now(), 
              role: `event-${timestamp}`, 
              event: message,
              source: "lambda-x",
              timestamp
            }
          })
        );
        
        console.log("Successfully stored event in DynamoDB");
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
