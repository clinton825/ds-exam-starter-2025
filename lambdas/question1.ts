import { APIGatewayProxyHandlerV2 } from "aws-lambda";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { 
  DynamoDBDocumentClient, 
  DeleteCommand, 
  GetCommand, 
  PutCommand, 
  QueryCommand, 
  ScanCommand 
} from "@aws-sdk/lib-dynamodb";
import { MovieCrewRole } from "../shared/types";

const client = createDDbDocClient();
const tableName = process.env.TABLE_NAME!;

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    // Extract HTTP method and path parameters
    const httpMethod = event.requestContext.http.method;
    const path = event.requestContext.http.path;
    const pathParams = event.pathParameters || {};
    
    // Handle different HTTP methods
    if (httpMethod === 'GET') {
      // New endpoint for exam question: /crew/{role}/movies/{movieId}
      if (path.includes('/crew/') && path.includes('/movies/')) {
        const role = pathParams.role;
        const movieId = parseInt(pathParams.movieId);
        
        if (!role || isNaN(movieId)) {
          return {
            statusCode: 400,
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ message: "Invalid parameters. Role and movieId are required." }),
          };
        }
        
        // Check for the verbose query parameter
        const queryParams = event.queryStringParameters || {};
        const verbose = queryParams.verbose === 'true';
        
        if (verbose) {
          // If verbose=true, return all crew members for the movie
          const result = await client.send(
            new QueryCommand({
              TableName: tableName,
              KeyConditionExpression: "movieId = :movieId",
              ExpressionAttributeValues: {
                ":movieId": movieId
              }
            })
          );
          
          return {
            statusCode: 200,
            headers: { "content-type": "application/json" },
            body: JSON.stringify(result.Items),
          };
        } else {
          // Default behavior: return only the requested role
          const result = await client.send(
            new GetCommand({
              TableName: tableName,
              Key: {
                movieId: movieId,
                role: role
              }
            })
          );
          
          if (!result.Item) {
            return {
              statusCode: 404,
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ message: `No ${role} found for movie ${movieId}` }),
            };
          }
          
          return {
            statusCode: 200,
            headers: { "content-type": "application/json" },
            body: JSON.stringify(result.Item),
          };
        }
      }
      
      // Original endpoints
      if (path === '/patha' || path === '/dev/patha') {
        // Get all movie crews (scan the table)
        const result = await client.send(
          new ScanCommand({
            TableName: tableName
          })
        );
        return {
          statusCode: 200,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify(result.Items),
        };
      } else if (pathParams.movieId) {
        // Get crews for a specific movie
        const movieId = parseInt(pathParams.movieId);
        
        if (pathParams.role) {
          // Get specific role for a movie
          const result = await client.send(
            new GetCommand({
              TableName: tableName,
              Key: {
                movieId: movieId,
                role: pathParams.role
              }
            })
          );
          
          if (!result.Item) {
            return {
              statusCode: 404,
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ message: "Crew role not found" }),
            };
          }
          
          return {
            statusCode: 200,
            headers: { "content-type": "application/json" },
            body: JSON.stringify(result.Item),
          };
        } else {
          // Get all roles for a movie
          const result = await client.send(
            new QueryCommand({
              TableName: tableName,
              KeyConditionExpression: "movieId = :movieId",
              ExpressionAttributeValues: {
                ":movieId": movieId
              }
            })
          );
          
          return {
            statusCode: 200,
            headers: { "content-type": "application/json" },
            body: JSON.stringify(result.Items),
          };
        }
      }
    } else if (httpMethod === 'POST') {
      // Create a new crew role
      const body = event.body ? JSON.parse(event.body) : {};
      
      // Validate required fields
      if (!body.movieId || !body.role || !body.names) {
        return {
          statusCode: 400,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ message: "Missing required fields: movieId, role, and names are required" }),
        };
      }
      
      const newCrew: MovieCrewRole = {
        movieId: body.movieId,
        role: body.role,
        names: body.names
      };
      
      await client.send(
        new PutCommand({
          TableName: tableName,
          Item: newCrew,
          ConditionExpression: "attribute_not_exists(movieId) AND attribute_not_exists(#r)",
          ExpressionAttributeNames: {
            "#r": "role"
          }
        })
      );
      
      return {
        statusCode: 201,
        headers: { "content-type": "application/json" },
        body: JSON.stringify(newCrew),
      };
    } else if (httpMethod === 'PUT') {
      // Update an existing crew role
      const body = event.body ? JSON.parse(event.body) : {};
      
      // Validate required fields
      if (!body.movieId || !body.role || !body.names) {
        return {
          statusCode: 400,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ message: "Missing required fields: movieId, role, and names are required" }),
        };
      }
      
      const updatedCrew: MovieCrewRole = {
        movieId: body.movieId,
        role: body.role,
        names: body.names
      };
      
      // Check if the item exists before updating
      const existingItem = await client.send(
        new GetCommand({
          TableName: tableName,
          Key: {
            movieId: updatedCrew.movieId,
            role: updatedCrew.role
          }
        })
      );
      
      if (!existingItem.Item) {
        return {
          statusCode: 404,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ message: "Crew role not found" }),
        };
      }
      
      await client.send(
        new PutCommand({
          TableName: tableName,
          Item: updatedCrew
        })
      );
      
      return {
        statusCode: 200,
        headers: { "content-type": "application/json" },
        body: JSON.stringify(updatedCrew),
      };
    } else if (httpMethod === 'DELETE') {
      // Delete a crew role
      if (!pathParams.movieId || !pathParams.role) {
        return {
          statusCode: 400,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ message: "Missing required path parameters: movieId and role" }),
        };
      }
      
      const movieId = parseInt(pathParams.movieId);
      
      await client.send(
        new DeleteCommand({
          TableName: tableName,
          Key: {
            movieId: movieId,
            role: pathParams.role
          }
        })
      );
      
      return {
        statusCode: 204,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      };
    }
    
    // Default response for unsupported methods
    return {
      statusCode: 405,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ message: "Method not allowed" }),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error: error.message }),
    };
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
