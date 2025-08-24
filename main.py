from fastapi import FastAPI, HTTPException
import boto3
from botocore.exceptions import ClientError
import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import List, Optional
from boto3.dynamodb.conditions import Key
from decimal import Decimal

app = FastAPI()

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
products_table = dynamodb.Table('products')
reviews_table = dynamodb.Table('reviews')
inventory_table = dynamodb.Table('inventory')

@strawberry.type
class Review:
    product_id: str
    review_id: str
    rating: int
    comment: str

@strawberry.type
class Inventory:
    product_id: str
    quantity_available: int
    location: str

@strawberry.type
class Product:
    product_id: str
    name: str | None = None
    price: float | None = None
    description: str | None = None

    # These are the nested resolvers that Strawberry automatically calls
    # when the query asks for them. They receive the parent `Product` object.
    @strawberry.field
    async def reviews(self) -> List[Review]:
        response = reviews_table.query(
            KeyConditionExpression=Key('product_id').eq(self.product_id)
        )
        return [Review(**item) for item in response.get('Items', [])]

    @strawberry.field
    async def inventory(self) -> Optional[Inventory]:
        response = inventory_table.get_item(
            Key={'product_id': self.product_id}
        )
        item = response.get('Item')
        if not item:
            # The resolver now correctly returns None, which is allowed
            # by the Optional[Inventory] type hint.
            return None 
        return Inventory(**item)

# Define the GraphQL Query
@strawberry.type
class Query:
    @strawberry.field
    async def get_product(self, product_id: str) -> Optional[Product]:
        try:
            response = products_table.get_item(Key={'product_id': product_id})
            item = response.get('Item')
            if not item:
                return None
            
            return Product(
                product_id=item['product_id'],
                name=item.get('name'),
                # Convert the Decimal from DynamoDB to float for Strawberry
                price=float(item.get('price')) if item.get('price') else None,
                description=item.get('description')
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    @strawberry.field
    async def list_products(self) -> List[Product]:
        try:
            response = products_table.scan()
            items = response.get('Items', [])
            
            # Continue scanning if there are more items
            while 'LastEvaluatedKey' in response:
                response = products_table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            # Convert DynamoDB Decimal types to floats for Strawberry
            products = [
                Product(
                    product_id=item['product_id'],
                    name=item.get('name'),
                    price=float(item.get('price')) if item.get('price') else None,
                    description=item.get('description')
                ) for item in items
            ]
            
            return products
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        
# Define the GraphQL Mutation type
@strawberry.type
class Mutation:
    @strawberry.mutation
    async def update_product(
        self,
        product_id: str,
        name: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[str] = None
    ) -> Product:
        try:
            update_expression_parts = []
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            if name is not None:
                update_expression_parts.append("#n = :n")
                expression_attribute_values[":n"] = name
                expression_attribute_names["#n"] = "name"
            
            if price is not None:
                update_expression_parts.append("price = :p")
                expression_attribute_values[":p"] = Decimal(str(price))
            
            if description is not None:
                update_expression_parts.append("#d = :d")
                expression_attribute_values[":d"] = description
                expression_attribute_names["#d"] = "description"
            
            update_expression = "SET " + ", ".join(update_expression_parts)

            response = products_table.update_item(
                Key={'product_id': product_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                ReturnValues="UPDATED_NEW"
            )

            updated_item = response.get('Attributes')
            if not updated_item:
                raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")

            # Convert the Decimal from DynamoDB to float before returning
            return Product(
                product_id=product_id,
                name=updated_item.get('name'),
                price=float(updated_item.get('price')) if updated_item.get('price') else None,
                description=updated_item.get('description')
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")