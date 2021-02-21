import boto3
import json
from boto3.dynamodb.conditions import Key

class table:
    db = None
    tableName = None
    table = None
    table_created = False

    def __init__(self):
        self.db  = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")
        print("Initialized")
    
    '''
        Create Table
    '''
    def createTable(self, tableName = None , KeySchema = None, AttributeDefinitions = None, ProvisionedThroughput = None):
        self.tableName = tableName
        try:
            table = self.db.create_table(
            TableName=tableName,
            KeySchema=KeySchema,
            AttributeDefinitions=AttributeDefinitions,
            ProvisionedThroughput=ProvisionedThroughput
            )
            self.table = table
            print(f'Created Table {self.table}')
        except:
            self.table = self.db.Table(self.tableName)
            print(f'{self.tableName} exists in db')
    
    '''
        Insert item into table
    '''
    def insert_data(self, path):
        with open(path) as f:
            data = json.load(f)
            for item in data:
                try:
                    self.table.put_item(Item = item)
                except Exception as e:
                    print(e)            
        print(f'Inserted Data into {self.tableName}')
    
    '''
        Get Item from table
    '''
    def getItem(self,key):
        try:
            response = self.table.get_item(Key = key)
            return response['Item']
        except Exception as e:
            print('Item not found')
            return None
    
    def updateItem(self,key,updateExpression, conditionExpression,expressionAttributes):
        try:
            response = self.table.update_item(
                Key = key,
                UpdateExpression = updateExpression,
                ConditionExpression = conditionExpression,
                ExpressionAttributeValues = expressionAttributes
            )
        except Exception as e:
            print(e)
    
    def deleteItem(self, key, conditionExpression, expressionAttributes):
        try:
            response = self.table.delete_item(
                Key = key,
                ConditionExpression = conditionExpression,
                ExpressionAttributeValues = expressionAttributes
            )
        except Exception as e:
            print(e)
    
    def query(self,projectExpression,expressionAttributes,keyExpression):
        try:
            response = self.table.query(
                ProjectionExpression = projectExpression,
                KeyConditionExpression= keyExpression,
            )
            return response['Items']
        except Exception as e:
            print(e)
            return None


    


'''
    For Testing Purposes
'''
if __name__ == '__main__':
    movies = table()

    primaryKey=[
        { 'AttributeName': 'year', 'KeyType': 'HASH'}, #Partition Key
        { 'AttributeName': 'title','KeyType': 'RANGE'} #Sort Key
    ] 
    attributeDataType=[
        { 'AttributeName': 'year', 'AttributeType': 'N'  }, #All Number Type
        { 'AttributeName': 'title', 'AttributeType': 'S' }, #String
    ]
    provisionedThroughput={ 'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10 }
    movies.createTable(
    tableName="Movie",
    KeySchema=primaryKey,AttributeDefinitions=attributeDataType,ProvisionedThroughput=provisionedThroughput)
    
    movies.insert_data(path = 'data.json')
    
    print("Before Update")
    print(movies.getItem(key = {'year' : 2019 , 'title': 'Title 3'}))
    
    upExp = "SET info.Info = :info , info.rating = :rating, info.Genre = list_append(info.Genre, :genre)"
    condExp = "info.Producer = :producer"
    expAttr = {
        ":info" : "Updated Information",
        ":rating" : 5,
        ":genre" : ["Legendary"],
        ":producer" : "Kevin Feige"
    }
    print("After Update")
    movies.updateItem({'year' : 2019 , 'title': 'Title 3'},upExp,condExp,expAttr)
    print(movies.getItem(key = {'year' : 2019 , 'title': 'Title 3'}))

    print("Before Delete")
    print(movies.getItem(key = {'title':'Title 2' , 'year': 2019}))
    print("After Delete")
    condExp = "info.Producer = :producer"
    expAttr = {':producer' : "ABC" }
    movies.deleteItem({'title':'Title 2' , 'year': 2019},condExp,expAttr)
    print(movies.getItem(key = {'title':'Title 2' , 'year': 2019}))
    
    print("Movies after 2019 with title starting with M")
    projection = "title"
    Keycondition = Key('year').eq(2020) & Key('title').begins_with('M')
    print(movies.query(projection,expAttr,Keycondition))
    
