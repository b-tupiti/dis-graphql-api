from fastapi import FastAPI, HTTPException
import boto3
from botocore.exceptions import ClientError
import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import List
from boto3.dynamodb.conditions import Key


app = FastAPI()

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
products_table = dynamodb.Table('products')
reviews_table = dynamodb.Table('reviews')

@strawberry.type
class Product:
    product_id: str
    name: str | None = None
    price: float | None = None
    description: str | None = None

@strawberry.type
class Review:
    product_id: str
    review_id: str
    rating: int
    comment: str

# Define the GraphQL Query
@strawberry.type
class Query:
    @strawberry.field
    async def get_product(self, product_id: str) -> Product:
        try:
            # Fetch item from DynamoDB
            response = products_table.get_item(Key={'product_id': product_id})
            item = response.get('Item')
            if not item:
                raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
            
            # Convert DynamoDB item to Product type
            return Product(
                product_id=item['product_id'],
                name=item.get('name'),
                price=item.get('price'),
                description=item.get('description')
            )
        except ClientError as e:
            # Handle DynamoDB-specific errors
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
        except Exception as e:
            # Handle unexpected errors
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    @strawberry.field
    async def get_reviews(self, product_id: str) -> List[Review]:
        """
        Fetches all reviews for a given product ID.
        This is the GraphQL equivalent of the previous REST endpoint.
        """
        try:
            # The resolver function uses the DynamoDB query operation.
            response = reviews_table.query(
                KeyConditionExpression=Key('product_id').eq(product_id)
            )
            
            # The 'Items' key contains the list of all matching reviews
            dynamo_reviews = response.get('Items', [])
            
            # We are now trusting that Strawberry will handle the Optional[int] correctly
            # and will not filter out any reviews from the response.
            reviews = [
                Review(
                    product_id=item.get('product_id'),
                    review_id=item.get('review_id'),
                    rating=item.get('rating'),
                    comment=item.get('comment')
                ) for item in dynamo_reviews
            ]
            
            # Since the return type is List[Review], we can return an empty list
            # if no reviews are found. Strawberry will handle the serialization.
            return reviews
        except ClientError as e:
            # Handle DynamoDB-specific errors
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
        except Exception as e:
            # Handle unexpected errors
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")